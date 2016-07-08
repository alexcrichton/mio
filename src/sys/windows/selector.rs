use std::{io, mem};
use std::cell::UnsafeCell;
use std::os::windows::prelude::*;
use std::sync::{Arc, Mutex};

use winapi::*;
use miow;
use miow::iocp::{CompletionPort, CompletionStatus};

use event::{Event, EventSet};
use poll::{self, ReadinessQueue};
use sys::windows::buffer_pool::BufferPool;
use sys::windows::from_raw_arc::FromRawArc;
use {Token, PollOpt};

/// The guts of the Windows event loop, this is the struct which actually owns
/// a completion port.
///
/// Internally this is just an `Arc`, and this allows handing out references to
/// the internals to I/O handles registered on this selector. This is
/// required to schedule I/O operations independently of being inside the event
/// loop (e.g. when a call to `write` is seen we're not "in the event loop").
pub struct Selector {
    inner: Arc<SelectorInner>,
}

pub struct Port {
    inner: Arc<SelectorInner>,
}

struct SelectorInner {
    /// The actual completion port that's used to manage all I/O
    port: CompletionPort,

    /// A pool of buffers usable by this selector.
    ///
    /// Primitives will take buffers from this pool to perform I/O operations,
    /// and once complete they'll be put back in.
    buffers: Mutex<BufferPool>,

    /// The readiness queue that we're associated with, used to actually drive
    /// all the event notificatons of this selector.
    queue: ReadinessQueue,
}

impl Selector {
    pub fn new(queue: ReadinessQueue) -> io::Result<Selector> {
        CompletionPort::new(8).map(|cp| {
            Selector {
                inner: Arc::new(SelectorInner {
                    port: cp,
                    buffers: Mutex::new(BufferPool::new(256)),
                    queue: queue,
                }),
            }
        })
    }

    pub fn select(&self, events: &mut Events, awakener: Token, timeout_ms: Option<usize>) -> io::Result<bool> {
        let mut ret = false;

        let timeout = timeout_ms.map(|ms| ms as u32);

        trace!("select; timeout={:?}", timeout);

        // Clear out the previous list of I/O events and get some more!
        events.events.truncate(0);
        let inner = &*self.inner;

        trace!("polling IOCP");
        let n = match inner.port.get_many(&mut events.statuses, timeout) {
            Ok(statuses) => statuses.len(),
            Err(ref e) if e.raw_os_error() == Some(WAIT_TIMEOUT as i32) => 0,
            Err(e) => return Err(e),
        };

        for status in events.statuses[..n].iter_mut() {
            if status.overlapped() as usize == 0 {
                if Token(status.token()) == awakener {
                    ret = true;
                    continue;
                }

                events.events.push(Event::new(EventSet::readable(),
                                              Token(status.token())));
                continue;
            }

            let callback = unsafe {
                (*(status.overlapped() as *mut Overlapped)).callback()
            };

            trace!("select; -> got overlapped");
            callback(status);
        }

        trace!("returning");
        Ok(ret)
    }

    /// Gets a reference to the underlying `CompletionPort` structure.
    pub fn port(&self) -> Port {
        Port { inner: self.inner.clone() }
    }
}

impl Port {
    /// Returns a reference to the underlying completion port
    pub fn port(&self) -> &CompletionPort {
        &self.inner.port
    }
}

impl SelectorInner {
    fn identical(&self, other: &SelectorInner) -> bool {
        (self as *const SelectorInner) == (other as *const SelectorInner)
    }
}

/// A registration is stored in each I/O object which keeps track of how it is
/// associated with a `Selector` above.
///
/// Once associated with a `Selector`, a registration can never be un-associated
/// (due to IOCP requirements). This is actually implemented through the
/// `poll::Registration` and `poll::SetReadiness` APIs to keep track of all the
/// level/edge/filtering business.
pub struct Registration {
    inner: Option<RegistrationInner>,
}

struct RegistrationInner {
    registration: poll::Registration,
    set_readiness: poll::SetReadiness,
    selector: Arc<SelectorInner>,
}

impl Registration {
    /// Creates a new blank registration ready to be inserted into an I/O object.
    ///
    /// Won't actually do anything until associated with an `Selector` loop.
    pub fn new() -> Registration {
        Registration {
            inner: None,
        }
    }

    /// Returns whether this registration has been associated with a selector
    /// yet.
    pub fn registered(&self) -> bool {
        self.inner.is_some()
    }

    /// Acquires a buffer with at least `size` capacity.
    ///
    /// If associated with a selector, this will attempt to pull a buffer from
    /// that buffer pool. If not associated with a selector, this will allocate
    /// a fresh buffer.
    pub fn get_buffer(&self, size: usize) -> Vec<u8> {
        match self.inner {
            Some(ref i) => i.selector.buffers.lock().unwrap().get(size),
            None => Vec::with_capacity(size),
        }
    }

    /// Returns a buffer to this registration.
    ///
    /// If associated with a selector, this will push the buffer back into the
    /// selector's pool of buffers. Otherwise this will just drop the buffer.
    pub fn put_buffer(&self, buf: Vec<u8>) {
        if let Some(ref i) = self.inner {
            i.selector.buffers.lock().unwrap().put(buf);
        }
    }

    /// Sets the readiness of this I/O object to a particular `set`.
    ///
    /// This is later used to fill out and respond to requests to `poll`. Note
    /// that this is all implemented through the `SetReadiness` structure in the
    /// `poll` module.
    pub fn set_readiness(&self, set: EventSet) {
        if let Some(ref i) = self.inner {
            trace!("set readiness to {:?}", set);
            let s = &i.set_readiness;
            s.set_readiness(set).expect("event loop disappeared?");
        }
    }

    /// Queries what the current readiness of this I/O object is.
    ///
    /// This is what's being used to generate events returned by `poll`.
    pub fn readiness(&self) -> EventSet {
        match self.inner {
            Some(ref i) => i.set_readiness.readiness(),
            None => EventSet::none(),
        }
    }

    /// Implementation of the `Evented::register` function essentially.
    ///
    /// Returns an error if we're already registered with another event loop,
    /// and otherwise just reassociates ourselves with the event loop to
    /// possible change tokens.
    pub fn register_socket(&mut self,
                           socket: &AsRawSocket,
                           selector: &Selector,
                           token: Token,
                           interest: EventSet,
                           opts: PollOpt) -> io::Result<()> {
        trace!("register {:?} {:?}", token, interest);
        try!(self.associate(selector, token, interest, opts));
        try!(selector.inner.port.add_socket(token.as_usize(), socket));
        Ok(())
    }

    /// Implementation of `Evented::reregister` function.
    pub fn reregister_socket(&mut self,
                             _socket: &AsRawSocket,
                             selector: &Selector,
                             token: Token,
                             interest: EventSet,
                             opts: PollOpt) -> io::Result<()> {
        trace!("reregister {:?} {:?}", token, interest);
        if self.inner.is_none() {
            return Err(other("cannot reregister unregistered socket"))
        }
        try!(self.associate(selector, token, interest, opts));
        Ok(())
    }

    fn associate(&mut self,
                 selector: &Selector,
                 token: Token,
                 events: EventSet,
                 opts: PollOpt) -> io::Result<()> {
        // To keep the same semantics as epoll, if I/O objects are interested in
        // being readable then they're also interested in listening for hup
        let events = if events.is_readable() {
            events | EventSet::hup()
        }  else {
            events
        };

        match self.inner {
            // Ensure that we're only ever associated with at most one event
            // loop. IOCP doesn't allow a handle to ever be associated with more
            // than one event loop.
            Some(ref i) if !i.selector.identical(&selector.inner) => {
                return Err(other("socket already registered"));
            }

            // If we're already registered, then just update the existing
            // registration.
            Some(ref i) => {
                trace!("updating existing registration node");
                poll::update_registration(&i.registration,
                                          &i.selector.queue,
                                          token,
                                          events,
                                          opts)
            }

            // Create a new registration and we'll soon be added to the
            // completion port for IOCP as well.
            None => {
                trace!("allocating new registration node");
                let (r, s) = poll::new_registration(&selector.inner.queue,
                                                    token,
                                                    events,
                                                    opts);
                self.inner = Some(RegistrationInner {
                    registration: r,
                    set_readiness: s,
                    selector: selector.inner.clone(),
                });
                Ok(())
            }
        }
    }

    /// Implementation of the `Evented::deregister` function.
    ///
    /// Doesn't allow registration with another event loop, just shuts down
    /// readiness notifications and such.
    pub fn deregister(&mut self, selector: &Selector) -> io::Result<()> {
        trace!("deregistering");
        match self.inner {
            Some(ref i) => {
                if !selector.inner.identical(&i.selector) {
                    return Err(other("socket already registered"));
                }
                try!(poll::deregister_registration(&i.registration,
                                                   &i.selector.queue));
                Ok(())
            }
            None => Err(other("socket not registered")),
        }
    }
}

fn other(s: &str) -> io::Error {
    io::Error::new(io::ErrorKind::Other, s)
}

#[derive(Debug)]
pub struct Events {
    /// Raw I/O event completions are filled in here by the call to `get_many`
    /// on the completion port above. These are then processed to run callbacks
    /// which figure out what to do after the event is done.
    statuses: Box<[CompletionStatus]>,

    /// Literal events returned by `get` to the upwards `EventLoop`. This file
    /// doesn't really modify this (except for the awakener), instead almost all
    /// events are filled in by the `ReadinessQueue` from the `poll` module.
    events: Vec<Event>,
}

impl Events {
    pub fn new() -> Events {
        // Use a nice large space for receiving I/O events (currently the same
        // as unix's 1024) and then also prepare the output vector to have the
        // same space.
        //
        // Note that it's possible for the output `events` to grow beyond 1024
        // capacity as it can also include deferred events, but that's certainly
        // not the end of the world!
        Events {
            statuses: vec![CompletionStatus::zero(); 1024].into_boxed_slice(),
            events: Vec::with_capacity(1024),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn get(&self, idx: usize) -> Option<Event> {
        self.events.get(idx).map(|e| *e)
    }

    pub fn push_event(&mut self, event: Event) {
        self.events.push(event);
    }
}

macro_rules! overlapped2arc {
    ($e:expr, $t:ty, $($field:ident).+) => (
        ::sys::windows::selector::Overlapped::cast_to_arc::<$t>($e,
                offset_of!($t, $($field).+))
    )
}

macro_rules! offset_of {
    ($t:ty, $($field:ident).+) => (
        &(*(0 as *const $t)).$($field).+ as *const _ as usize
    )
}

pub type Callback = fn(&CompletionStatus);

/// See sys::windows module docs for why this exists.
///
/// The gist of it is that `Selector` assumes that all `OVERLAPPED` pointers are
/// actually inside one of these structures so it can use the `Callback` stored
/// right after it.
///
/// We use repr(C) here to ensure that we can assume the overlapped pointer is
/// at the start of the structure so we can just do a cast.
#[repr(C)]
pub struct Overlapped {
    inner: UnsafeCell<miow::Overlapped>,
    callback: Callback,
}

impl Overlapped {
    pub fn new(cb: Callback) -> Overlapped {
        Overlapped {
            inner: UnsafeCell::new(miow::Overlapped::zero()),
            callback: cb,
        }
    }

    pub unsafe fn get_mut(&self) -> &mut miow::Overlapped {
        &mut *self.inner.get()
    }

    pub unsafe fn cast_to_arc<T>(overlapped: *mut miow::Overlapped,
                                 offset: usize) -> FromRawArc<T> {
        debug_assert!(offset < mem::size_of::<T>());
        FromRawArc::from_raw((overlapped as usize - offset) as *mut T)
    }

    pub unsafe fn callback(&self) -> &Callback {
        &self.callback
    }
}

// Overlapped's APIs are marked as unsafe Overlapped's APIs are marked as
// unsafe as they must be used with caution to ensure thread safety. The
// structure itself is safe to send across threads.
unsafe impl Send for Overlapped {}
unsafe impl Sync for Overlapped {}
