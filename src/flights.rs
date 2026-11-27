//! One running agent reply.
//!
//! Cancel and steer are per model inside a channel and thread, so another
//! model in the same turn keeps going. A steer waits until the current model
//! call returns. It does not abort that call and it does not rebuild context.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

#[derive(Clone)]
pub struct Ticket {
    pub id: u64,
    pub cancel: watch::Receiver<bool>,
    steers: Arc<Mutex<Vec<String>>>,
}

impl Ticket {
    pub fn take_steers(&self) -> Vec<String> {
        let Ok(mut guard) = self.steers.lock() else {
            return Vec::new();
        };
        std::mem::take(&mut *guard)
    }

    pub async fn until_cancelled(&mut self) {
        loop {
            if *self.cancel.borrow() {
                return;
            }
            if self.cancel.changed().await.is_err() {
                return;
            }
        }
    }
}

struct Slot {
    id: u64,
    channel: String,
    thread: String,
    model: String,
    cancel: watch::Sender<bool>,
    steers: Arc<Mutex<Vec<String>>>,
}

pub struct Board {
    next: AtomicU64,
    inner: Mutex<Vec<Slot>>,
}

impl Default for Board {
    fn default() -> Self {
        Self::new()
    }
}

impl Board {
    pub fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
            inner: Mutex::new(Vec::new()),
        }
    }

    pub fn begin(&self, channel: &str, thread: &str, model: &str) -> Ticket {
        let (tx, rx) = watch::channel(false);
        let steers = Arc::new(Mutex::new(Vec::new()));
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut guard) = self.inner.lock() {
            guard.push(Slot {
                id,
                channel: channel.to_string(),
                thread: thread.to_string(),
                model: model.to_string(),
                cancel: tx,
                steers: steers.clone(),
            });
        }
        Ticket {
            id,
            cancel: rx,
            steers,
        }
    }

    pub fn end(&self, id: u64) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.retain(|slot| slot.id != id);
        }
    }

    /// Stop every pending reply of this model. Returns how many were signalled.
    pub fn cancel(&self, channel: &str, thread: &str, model: &str) -> usize {
        let Ok(guard) = self.inner.lock() else {
            return 0;
        };
        let mut n = 0;
        for slot in guard.iter() {
            if slot.channel == channel && slot.thread == thread && slot.model == model {
                if slot.cancel.send(true).is_ok() {
                    n += 1;
                }
            }
        }
        n
    }

    /// `model` empty steers every running reply in the channel and thread.
    pub fn steer(&self, channel: &str, thread: &str, model: &str, text: &str) -> usize {
        let text = text.trim();
        if text.is_empty() {
            return 0;
        }
        let Ok(guard) = self.inner.lock() else {
            return 0;
        };
        let mut n = 0;
        for slot in guard.iter() {
            if slot.channel != channel || slot.thread != thread {
                continue;
            }
            if !model.is_empty() && slot.model != model {
                continue;
            }
            if let Ok(mut q) = slot.steers.lock() {
                if q.len() < 8 {
                    q.push(text.to_string());
                    n += 1;
                }
            }
        }
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancel_signals_only_the_named_model() {
        let board = Board::new();
        let huge = board.begin("c", "main", "huge/qwen");
        let creative = board.begin("c", "main", "creative/deepseek");
        assert_eq!(board.cancel("c", "main", "huge/qwen"), 1);
        assert!(*huge.cancel.borrow());
        assert!(!*creative.cancel.borrow());
        assert_eq!(
            board.steer("c", "main", "creative/deepseek", "narrow the parser"),
            1
        );
        assert_eq!(
            creative.take_steers(),
            vec!["narrow the parser".to_string()]
        );
        assert!(huge.take_steers().is_empty());
        assert_eq!(board.cancel("c", "other", "huge/qwen"), 0);
    }
}
