/*
 * Written by Brian Goetz and Tim Peierls, with help from
 * Joshua Bloch, Joseph Bowbeer, David Holmes, and Doug Lea,
 * and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
package jcip.rrlock;

import java.util.concurrent.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.atomic.*;


/**
 * <p> A generalization of a two gender bathroom lock to
 * many genders. An instance of RRLock provides a Lock
 * object for each gender. The lock associated with a
 * gender may acquired by multiple times by multiple
 * threads, but only if all other genders have released
 * their associated locks completely. </p>
 *
 * <p> There are no guarantees about acquisition order.
 * Conditions are not supported. The returned locks are
 * reentrant. </p>
 *
 * <p> Sample code for managing a regular (2 gender) bathroom
 * under a policy that dictates that males who cannot acquire
 * the lock after a minute must use alternative facilities and
 * females who cannot acquire the lock must block indefinitely
 * in the hallway: </p>
 *
 * <pre>
 * abstract class BathroomManager {
 *     enum Gender { MALE, FEMALE }
 *     private final RRLock<Gender> wc = new RRLock<Gender>();
 *     void male() {
 *         Lock lock = wc.lockFor(MALE);
 *         if (lock.tryLock(1, MINUTE))
 *             try { useBathroom(MALE); }
 *             finally { lock.unlock(); }
 *         else
 *             useAlternativeFacilities();
 *     }
 *     void female() throws InterruptedException {
 *         Lock lock = wc.lockFor(FEMALE);
 *         lock.lockInterruptibly();
 *         try { useBathroom(FEMALE); }
 *         finally { lock.unlock(); }
 *     }
 *     abstract void useBathroom(Gender g);
 *     abstract void useAlternativeFacilities();
 * }
 * </pre>
 *
 * <h2> Implementation limitations </h2>
 *
 * <p> This implementation uses a single int to encode two
 * pieces of information: the origin-1 index of the gender
 * that has the lock, or 0 if no gender has the lock; and
 * a count of the number of unlocks needed to release the
 * lock completely. </p>
 *
 * <p> This means that number of bits used to represent the
 * largest index plus the number of bits used to represent
 * the count must be at most 32. The implementation does not
 * check to see if a lock acquistion breaks this rule. </p>
 *
 * @author Tim Peierls
 */
public final class RRLock<G> {

    private static final int DEFAULT_IMPL = 1;

    /**
     * Creates a new instance of RRLock using the default
     * internal synchronizer implementation.
     */
    public RRLock() {
        this(DEFAULT_IMPL);
    }

    /**
     * Creates a new instance of RRLock using the internal
     * synchronizer implementation corresponding to the
     * <tt>syncImpl</tt> argument.
     */
    public RRLock(int syncImpl) {
        switch (syncImpl) {
          case 1: this.sync = new Sync1(); break;
          case 2: this.sync = new Sync2(); break;
          case 3: this.sync = new Sync3(); break;
          default:
              throw new IllegalArgumentException("unknown implementation index");
        }
    }

    /**
     * Returns the lock associated with the given gender.
     */
    public Lock lockFor(G gender) {
        Lock lock = map.get(gender);
        if (lock == null) {
            synchronized (sync) {
                lock = new LockImpl(); // uses current gender count
                Lock prev = map.putIfAbsent(gender, lock);
                if (prev == null)
                    ngenders.incrementAndGet(); // successful, bump gender count
                else
                    lock = prev;                // unsuccessful, use previous lock
            }
        }
        return lock;
    }


    private final ConcurrentMap<G, Lock> map =
        new ConcurrentHashMap<G, Lock>();

    /**
     * All lock behavior is defined in terms of a single
     * AQS sync object. Locks pass their index as the argument
     * value to acquire and release methods.
     */
    private class LockImpl implements Lock {
        public void lock() {
            sync.acquireShared(index);
        }

        public void lockInterruptibly() throws InterruptedException {
            sync.acquireSharedInterruptibly(index);
        }

        public boolean tryLock() {
            return sync.tryAcquireShared(index) > 0;
        }

        public boolean tryLock(long timeout, TimeUnit unit)
            throws InterruptedException {
            return sync.tryAcquireSharedNanos(index, unit.toNanos(timeout));
        }

        public void unlock() {
            sync.releaseShared(index);
        }

        public Condition newCondition() {
            throw new UnsupportedOperationException();
        }

        LockImpl() {
            this.index = ngenders.get();
        }

        private final int index;
    }


    private final AtomicInteger ngenders = new AtomicInteger(1);
    private final Sync sync;

    /**
     * There is no exclusive locking, so we only need to
     * define <tt>tryAcquireShared</tt> and tryReleaseShared</tt>.
     */
    private abstract class Sync extends AbstractQueuedSynchronizer {

        protected abstract int tryAcquireShared(int index);
        protected abstract boolean tryReleaseShared(int index);

        /**
         * Returns a bitmask with ones in the rightmost n positions,
         * where n is the number of bits needed to represent the
         * highest index seen so far.
         */
        final int indexMask() {
            return (Integer.highestOneBit(ngenders.get()-1) << 1) - 1;
        }

        /**
         * State 0 means no one holds the lock, and so it is
         * always acquirable. Otherwise, the state's extracted
         * index must match the current would-be acquirer's index.
         */
        final boolean canAcquire(int index, int s, int mask) {
            return s == 0 ? true : (index == extractIndex(s, mask));
        }

        /**
         * If the state's extracted index does not match the
         * current would-be releaser's index, we are in an
         * illegal state.
         */
        final boolean canRelease(int index, int s, int mask) {
            return index == extractIndex(s, mask);
        }

        /**
         * If we already have the lock, then we just add 1.
         * If we are going from 0, we have to combine the
         * count of 1 with our index.
         *
         * @to.do Check for overflow against mask and throw
         * IllegalMonitorStateException on overflow.
         */
        final int acquiredState(int index, int s, int mask) {
            return s == 0 ? combineIndexAndCount(index, 1) : ++s;
        }

        /**
         * If the count drops to zero, the new state is plain
         * zero, meaning no one holds the lock. Otherwise just
         * use the decremented value.
         */
        final int releasedState(int index, int s, int mask) {
            return extractCount(--s, mask) == 0 ? 0 : s;
        }

        /**
         * Count is stored flush-right, and index is stored
         * reversed, flush-left.
         */
        final int combineIndexAndCount(int index, int count) {
            return count | Integer.reverse(index);
        }

        /**
         * Index is stored reversed, flush-left.
         */
        final int extractIndex(int s, int mask) {
            return Integer.reverse(s) & mask;
        }

        /**
         * Mask out the index, which is stored reversed,
         * flush-left, and the remaining bits are the
         * count.
         */
        final int extractCount(int s, int mask) {
            return s & ~Integer.reverse(mask);
        }
    }

    /**
     * This implementation of Sync is designed around
     * the common case of a relatively static gender
     * set. It does two volatile reads, no matter how
     * much contention there is.
     */
    private class Sync1 extends Sync {
        protected int tryAcquireShared(int index) {
            int mask = indexMask();
            while (true) {
                int s = getState();
                if (!canAcquire(index, s, mask))
                    return -1;
                int ns = acquiredState(index, s, mask);
                if (compareAndSetState(s, ns)) {
                    int omask = mask;
                    mask = indexMask();
                    if (omask == mask)  // check if mask changed
                        return 1;       // if not, we're done
                    do {                // if so, release and try again
                        s = getState();
                        ns = releasedState(index, s, mask);
                    } while (!compareAndSetState(s, ns));
                }
            }
        }

        protected boolean tryReleaseShared(int index) {
            int mask = indexMask();
            while (true) {
                int s = getState();
                if (!canRelease(index, s, mask))
                    throw new IllegalMonitorStateException();
                int ns = releasedState(index, s, mask);
                if (compareAndSetState(s, ns))
                    return true;
            }
        }
    }

    /**
     * This implementation of Sync may do many volatile
     * reads under heavy contention, but is simpler.
     */
    private class Sync2 extends Sync {
        protected int tryAcquireShared(int index) {
            while (true) {
                int mask = indexMask();
                int s = getState();
                if (!canAcquire(index, s, mask))
                    return -1;
                int ns = acquiredState(index, s, mask);
                if (compareAndSetState(s, ns))
                    return 1;
            }
        }

        protected boolean tryReleaseShared(int index) {
            int mask = indexMask();
            while (true) {
                int s = getState();
                if (!canRelease(index, s, mask))
                    throw new IllegalMonitorStateException();
                int ns = releasedState(index, s, mask);
                if (compareAndSetState(s, ns))
                    return true;
            }
        }
    }

    /**
     * This version is like Sync2, but without the
     * spin optimization in tryAcquireShared(int).
     */
    private class Sync3 extends Sync {
        protected int tryAcquireShared(int index) {
            int mask = indexMask();
            int s = getState();
            if (!canAcquire(index, s, mask))
                return -1;
            int ns = acquiredState(index, s, mask);
            return compareAndSetState(s, ns) ? 1 : -1;
        }

        protected boolean tryReleaseShared(int index) {
            int mask = indexMask();
            while (true) {
                int s = getState();
                if (!canRelease(index, s, mask))
                    throw new IllegalMonitorStateException();
                int ns = releasedState(index, s, mask);
                if (compareAndSetState(s, ns))
                    return true;
            }
        }
    }
}
