/*
 * Written by Brian Goetz and Tim Peierls, with help from
 * Joshua Bloch, Joseph Bowbeer, David Holmes, and Doug Lea,
 * and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
package jcip.rrlock;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;


/**
 * Locks with owner generalized from thread to object.
 * Non-fair, reentrant, exclusive, supports interruption,
 * supports condition.
 *
 * @author Tim Peierls
 */
public final class OwnedLock<T> {

    /**
     * Returns an exclusive, reentrant lock associated with the
     * given owner.
     */
    public Lock lockFor(T owner) {
        return new LockImpl(owner);
    }


    private class LockImpl implements Lock {
        public void lock() {
            currentOwner.set(owner);
            try {
                sync.acquire(1);
            } finally {
                currentOwner.set(null);
            }
        }

        public void lockInterruptibly() throws InterruptedException {
            currentOwner.set(owner);
            try {
                sync.acquireInterruptibly(1);
            } finally {
                currentOwner.set(null);
            }
        }

        public boolean tryLock() {
            currentOwner.set(owner);
            try {
                return sync.tryAcquire(1);
            } finally {
                currentOwner.set(null);
            }
        }

        public boolean tryLock(long timeout, TimeUnit unit)
            throws InterruptedException {
            currentOwner.set(owner);
            try {
                long nanos = unit.toNanos(timeout);
                return sync.tryAcquireNanos(1, nanos);
            } finally {
                currentOwner.set(null);
            }
        }

        public void unlock() {
            currentOwner.set(owner);
            try {
                sync.release(1);
            } finally {
                currentOwner.set(null);
            }
        }

        public Condition newCondition() {
            return sync.newCondition(owner);
        }

        LockImpl(T owner) { this.owner = owner; }

        private final T owner;
    }


    private final ThreadLocal<T> currentOwner = new ThreadLocal<T>();
    private final Sync sync = new Sync();

    /**
     * Only allowing exclusive locking, so we only need to
     * define <tt>tryAcquire</tt> and tryRelease</tt>.
     * State is number of nested acquisitions, request
     * argument is number of times to acquire/release.
     */
    private class Sync extends AbstractQueuedSynchronizer {

        protected boolean tryAcquire(int acquires) {
            if (compareAndSetState(0, acquires)) {
                owner = currentOwner.get();
                return true;
            }
            if (!isHeldExclusively())
                return false;

            setState(getState() + acquires);
            return true;
        }

        protected boolean tryRelease(int releases) {
            if (!isHeldExclusively())
                throw new IllegalMonitorStateException();

            if (compareAndSetState(releases, 0)) {
                owner = null;
                return true;
            } else {
                setState(getState() - releases);
                return false;
            }
        }

        protected boolean isHeldExclusively() {
            return getState() > 0 && owner == currentOwner.get();
        }

        Condition newCondition(final T owner) {
            return new Condition() {
                public void await() throws InterruptedException {
                    currentOwner.set(owner);
                    try {
                        cond.await();
                    } finally {
                        currentOwner.set(null);
                    }
                }
                public void awaitUninterruptibly() {
                    currentOwner.set(owner);
                    try {
                        cond.awaitUninterruptibly();
                    } finally {
                        currentOwner.set(null);
                    }
                }
                public boolean await(long time, TimeUnit unit)
                    throws InterruptedException {
                    currentOwner.set(owner);
                    try {
                        return cond.await(time, unit);
                    } finally {
                        currentOwner.set(null);
                    }
                }
                public long awaitNanos(long nanos)
                    throws InterruptedException {
                    currentOwner.set(owner);
                    try {
                        return cond.awaitNanos(nanos);
                    } finally {
                        currentOwner.set(null);
                    }
                }
                public boolean awaitUntil(Date date)
                    throws InterruptedException {
                    currentOwner.set(owner);
                    try {
                        return cond.awaitUntil(date);
                    } finally {
                        currentOwner.set(null);
                    }
                }
                public void signal() {
                    currentOwner.set(owner);
                    try {
                        cond.signal();
                    } finally {
                        currentOwner.set(null);
                    }
                }
                public void signalAll() {
                    currentOwner.set(owner);
                    try {
                        cond.signalAll();
                    } finally {
                        currentOwner.set(null);
                    }
                }
                private final Condition cond = new ConditionObject();
            };
        }

        private transient T owner;
    }
}
