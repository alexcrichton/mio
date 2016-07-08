use std::sync::Mutex;

use miow::iocp::CompletionStatus;
use sys::windows::selector::Port;
use {io, poll, Evented, EventSet, Poll, PollOpt, Token};

pub struct Awakener {
    inner: Mutex<Option<AwakenerInner>>,
}

struct AwakenerInner {
    token: Token,
    port: Port,
}

impl Awakener {
    pub fn new() -> io::Result<Awakener> {
        Ok(Awakener {
            inner: Mutex::new(None),
        })
    }

    pub fn wakeup(&self) -> io::Result<()> {
        // Each wakeup notification has NULL as its `OVERLAPPED` pointer to
        // indicate that it's from this awakener and not part of an I/O
        // operation. This is specially recognized by the selector.
        //
        // If we haven't been registered with an event loop yet just silently
        // succeed.
        let inner = self.inner.lock().unwrap();
        if let Some(inner) = inner.as_ref() {
            let status = CompletionStatus::new(0,
                                               inner.token.as_usize(),
                                               0 as *mut _);
            try!(inner.port.port().post(status));
        }
        Ok(())
    }

    pub fn cleanup(&self) {
        // noop
    }
}

impl Evented for Awakener {
    fn register(&self, poll: &Poll, token: Token, events: EventSet,
                opts: PollOpt) -> io::Result<()> {
        assert_eq!(opts, PollOpt::edge());
        assert_eq!(events, EventSet::readable());
        *self.inner.lock().unwrap() = Some(AwakenerInner {
            port: poll::selector(poll).port(),
            token: token,
        });
        Ok(())
    }

    fn reregister(&self, poll: &Poll, token: Token, events: EventSet,
                  opts: PollOpt) -> io::Result<()> {
        self.register(poll, token, events, opts)
    }

    fn deregister(&self, _poll: &Poll) -> io::Result<()> {
        *self.inner.lock().unwrap() = None;
        Ok(())
    }
}
