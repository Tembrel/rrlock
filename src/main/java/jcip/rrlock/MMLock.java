/*
 * Written by Brian Goetz and Tim Peierls, with help from
 * Joshua Bloch, Joseph Bowbeer, David Holmes, and Doug Lea,
 * and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
package jcip.rrlock;

import java.util.concurrent.*;
import java.util.concurrent.locks.*;

/**
 * Lock container that supports four modes:
 * read, write, intent-read, and intent-write.
 * The lock compatibility matrix is:
 * <pre>
 *     IR R  IW W
 * IR           X
 *  R        X  X
 * IW     X     X
 *  W  X  X  X  X
 * </pre>
 *
 * Non-fair acquisition, reentrant, no upgrade/downgrade,
 * interruptible acquisition, condition support for write
 * lock only, no instrumentation.
 *
 * @author Tim Peierls
 */
public final class MMLock<T> {

    public Lock readLock() {
        return rlock;
    }

    public Lock writeLock() {
        return wlock;
    }

    public Lock intentReadLock() {
        return irlock;
    }

    public Lock intentWriteLock() {
        return iwlock;
    }

    public Lock incrementLock() {
        return iwlock;
    }


    private abstract class SharedLock implements Lock {
        public void lock() {
            sync.acquireShared(request());
        }

        public void lockInterruptibly() throws InterruptedException {
            sync.acquireSharedInterruptibly(request());
        }

        public boolean tryLock() {
            return sync.tryAcquireShared(request()) >= 0;
        }

        public boolean tryLock(long timeout, TimeUnit unit)
            throws InterruptedException {
            long nanos = unit.toNanos(timeout);
            return sync.tryAcquireSharedNanos(request(), nanos);
        }

        public void unlock() {
            sync.releaseShared(request());
        }

        public Condition newCondition() {
            throw new UnsupportedOperationException();
        }

        abstract int request();
    }

    private class IntentReadLock extends SharedLock {
        int request() {
            return IR_REQ;
        }
    }

    private class IntentWriteLock extends SharedLock {
        int request() {
            return IR_IW_LOCKED | 1;
        }
    }

    private class ReadLock extends SharedLock {
        int request() {
            return IR_READ_LOCKED | 1;
        }
    }

    private class ExclusiveLock extends SharedLock {
        int request() {
            return EXCL_LOCKED | 1;
        }

        public void lock() {
            sync.acquire(request());
        }

        public void lockInterruptibly() throws InterruptedException {
            sync.acquireInterruptibly(request());
        }

        public boolean tryLock() {
            return sync.tryAcquire(request());
        }

        public boolean tryLock(long timeout, TimeUnit unit)
            throws InterruptedException {
            long nanos = unit.toNanos(timeout);
            return sync.tryAcquireNanos(request(), nanos);
        }

        public void unlock() {
            sync.release(request());

        }

        public Condition newCondition() {
            return sync.newCondition();
        }
    }

    /**
     * Synchronizer states fall into four categories:
     * <ol>
     * <li> Not held by any thread </li>
     * <li> Held by intent-readers and/or readers <li>
     * <li> Held by intent-readers and/or intent-writers </li>
     * <li> Held exclusively </li>
     * </ol>
     * The category is encoded as the top two bits of the state.
     * <p>
     * The remaining 30 bits of state are used to hold either one
     * or two counts, the number of lock holders. When representing
     * two counts, the lowest 15 bits hold one count and the remaining
     * 15 bits hold the other count.
     * <p>
     * In state 1, the count field is not used.
     * <p>
     * In states 2 and 3, there are two counts, and the intent-readers
     * count is in the higher 15 bits.
     * <p>
     * In state 4, there is one count.
     * <p>
     * The argument passed to tryAcquire and tryRelease is always a
     * valid state with one nonzero count.
     */
    private static final class Sync extends AbstractQueuedSynchronizer {

        protected boolean tryAcquire(int request) {
            if (compareAndSetState(FREE, request)) {
                owner = Thread.currentThread();
                return true;
            }
            if (!isHeldExclusively())
                return false;
            int s = getState();
            int c = exclusiveCount(s);
            if (c == EXCL_COUNT)
                throw new Error(MAX_LOCKS_EXCEEDED);
            setState(request+c);
            return true;
        }

        protected boolean tryRelease(int request) {
            if (!isHeldExclusively())
                throw new IllegalMonitorStateException();
            int s = getState();
            if (s == request) {
                owner = null;
                setState(FREE);
                return true;
            } else {
                int c = exclusiveCount(request);
                setState(s-c);
                return false;
            }
        }

        protected int tryAcquireShared(int request) {
            while (true) {
                int s = getState();
                if (!canAcquireShared(s, request))
                    return -1;
                int ns = acquiredState(s, request);
                if (compareAndSetState(s, ns))
                    return 1;
            }
        }

        protected boolean tryReleaseShared(int request) {
            while (true) {
                int s = getState();
                if (!canReleaseShared(s, request))
                    return false;
                int ns = releasedState(s, request);
                if (compareAndSetState(s, ns))
                    return ns == FREE;
            }
        }

        protected boolean isHeldExclusively() {
            return isExclusive(getState()) &&
                   owner == Thread.currentThread();
        }

        boolean canAcquireShared(int s, int request) {
            if (s == FREE)
                return true;
            if (isExclusive(s))
                return false;
            if (isRead(s) && isIntentWrite(request))
                return false;
            if (isIntentWrite(s) && isRead(request))
                return false;

            return true;
        }

        boolean canReleaseShared(int s, int request) {
            if (s == FREE || isExclusive(s))
                throw new IllegalMonitorStateException();
            if (isRead(s) && isIntentWrite(request))
                throw new IllegalMonitorStateException();
            if (isIntentWrite(s) && isRead(request))
                throw new IllegalMonitorStateException();

            return true;
        }

        int acquiredState(int s, int request) {
            if (s == FREE)
                return request;

            if ((lowerCountAtMax(s) && lowerCount(request) != 0) ||
                (upperCountAtMax(s) && upperCount(request) != 0))
                throw new Error(MAX_LOCKS_EXCEEDED);

            int c = exclusiveCount(s); // == upper|lower
            return request+c;
        }

        int releasedState(int s, int request) {
            if (s == request)
                return FREE;
            if (s == ALT_IR_REQ && request == IR_REQ)
                return FREE;

            if (lowerCount(s) < lowerCount(request) ||
                upperCount(s) < upperCount(request))
                throw new IllegalMonitorStateException();

            int c = exclusiveCount(request); // == upper|lower
            return s-c;
        }

        boolean isExclusive(int s) {
            return (s & MODE_MASK) == EXCL_LOCKED;
        }

        boolean isRead(int s) {
            return (s & MODE_MASK) == IR_READ_LOCKED
                && lowerCount(s) != 0;
        }

        boolean isIntentWrite(int s) {
            return (s & MODE_MASK) == IR_IW_LOCKED
                && lowerCount(s) != 0;
        }

        boolean isIntentRead(int s) {
            int mode = s & MODE_MASK;
            return (mode == IR_IW_LOCKED || mode == IR_READ_LOCKED)
                && upperCount(s) != 0;
        }

        int exclusiveCount(int s) {
            return s & EXCL_COUNT;
        }

        int lowerCount(int s) {
            return s & LOWER_COUNT;
        }

        int upperCount(int s) {
            return (s & UPPER_COUNT) >> IR_SHIFT;
        }

        boolean lowerCountAtMax(int s) {
            return (s & LOWER_COUNT) == LOWER_COUNT;
        }

        boolean upperCountAtMax(int s) {
            return (s & UPPER_COUNT) == UPPER_COUNT;
        }

        Condition newCondition() {
            return new ConditionObject();
        }

        transient Thread owner = null;
    }


    private final Lock rlock = new ReadLock();
    private final Lock wlock = new ExclusiveLock();
    private final Lock irlock = new IntentReadLock();
    private final Lock iwlock = new IntentWriteLock();
    private final Sync sync = new Sync();


    private static final int MODE_MASK      = 0xC0000000;
    private static final int EXCL_COUNT     = ~MODE_MASK;
    private static final int IR_SHIFT       = 15;
    private static final int LOWER_COUNT    = 0x00007FFF;
    private static final int UPPER_COUNT    = LOWER_COUNT << IR_SHIFT;

    private static final int FREE           = 0;
    private static final int IR_READ_LOCKED = 0x80000000;
    private static final int IR_IW_LOCKED   = 0x40000000;
    private static final int EXCL_LOCKED    = IR_READ_LOCKED | IR_IW_LOCKED;

    private static final int IR_REQ         = IR_READ_LOCKED | (1<<IR_SHIFT);
    private static final int ALT_IR_REQ     = IR_IW_LOCKED | (1<<IR_SHIFT);

    private static final String MAX_LOCKS_EXCEEDED =
        "Maximum lock count exceeded";
}
