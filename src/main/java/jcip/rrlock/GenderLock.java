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
 * <p> A generalization of a two gender bathroom lock to
 * many genders. An instance of GenderLock provides a Lock
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
 *     enum G { MALE, FEMALE }
 *     private final GenderLock<G> wc =
 *         new GenderLock<G>(G.values());
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
 *     abstract void useBathroom(G g);
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
 * <h2> References </h2>
 *
 * <p> Gender lock provides a kind of "room synchronization".
 * See http://www-2.cs.cmu.edu/afs/cs/project/pscico/www/rooms.html
 * and http://www.cs.brown.edu/courses/cs176/ for more about
 * this. </p>
 *
 * @author Tim Peierls
 */
public final class GenderLock<G> {

    /**
     * Creates a new instance of GenderLock using the
     * given genders.
     */
    public GenderLock(G[] genders) {
        int n = 1;
        for (G gender : genders)
            this.map.put(gender, new LockImpl(n++));
        this.mask = indexMask(n);
    }

    /**
     * Creates a new instance of GenderLock using the
     * given genders.
     */
    public GenderLock(Collection<G> genders) {
        int n = 1;
        for (G gender : genders)
            this.map.put(gender, new LockImpl(n++));
        this.mask = indexMask(n);
    }

    /**
     * Returns the lock associated with the given gender.
     */
    public Lock lockFor(G gender) {
        Lock result = map.get(gender);
        if (result == null)
            throw new IllegalArgumentException();
        return result;
    }

    /**
     * Associate a handler for the given gender that will
     * be called that gender's lock is unlocked completely.
     */
    public void setExitHandler(G gender, Runnable handler) {
        LockImpl lock = map.get(gender);
        handlers.put(lock.index, handler);
    }


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
            if (sync.releaseShared(index)) {
                Runnable handler = handlers.get(index);
                if (handler != null) handler.run();
            }
        }

        public Condition newCondition() {
            throw new UnsupportedOperationException();
        }

        LockImpl(int index) {
            this.index = Integer.reverse(index);
        }

        private final int index;
    }

    /**
     * There is no exclusive locking, so we only need to
     * define <tt>tryAcquireShared</tt> and tryReleaseShared</tt>.
     */
    private class Sync extends AbstractQueuedSynchronizer {

        protected int tryAcquireShared(int index) {
            while (true) {
                int s = getState();
                if (!canAcquire(index, s))
                    return -1;
                int ns = acquiredState(index, s);
                if (compareAndSetState(s, ns))
                    return 1;
            }
        }

        protected boolean tryReleaseShared(int index) {
            while (true) {
                int s = getState();
                if (!canRelease(index, s))
                    throw new IllegalMonitorStateException();
                int ns = releasedState(index, s);
                if (compareAndSetState(s, ns))
                    return ns == 0;
            }
        }

        /**
         * State 0 means no one holds the lock, and so it is
         * always acquirable. Otherwise, the state's extracted
         * index must match the current would-be acquirer's index.
         */
        final boolean canAcquire(int index, int s) {
            return s == 0 ? true : (index == extractIndex(s));
        }

        /**
         * If the state's extracted index does not match the
         * current would-be releaser's index, we are in an
         * illegal state.
         */
        final boolean canRelease(int index, int s) {
            return index == extractIndex(s);
        }

        /**
         * If we already have the lock, then we just add 1.
         * If we are going from 0, we have to combine the
         * count of 1 with our index.
         *
         * @to.do Check for overflow against mask and throw
         * IllegalMonitorStateException on overflow.
         */
        final int acquiredState(int index, int s) {
            if (s == 0)
                return combineIndexAndCount(index, 1);
            if (index == extractIndex(++s))
                return s;
            // Incrementing state changed the index, which
            // means the count bits overflowed.
            throw new Error("Maximum read count exceeded");
        }

        /**
         * If the count drops to zero, the new state is plain
         * zero, meaning no one holds the lock. Otherwise just
         * use the decremented value.
         */
        final int releasedState(int index, int s) {
            return extractCount(--s) == 0 ? 0 : s;
        }

        /**
         * Count is stored flush-right, and index is stored
         * reversed, flush-left.
         */
        final int combineIndexAndCount(int index, int count) {
            return count | index;
        }

        /**
         * Index is stored reversed, flush-left.
         */
        final int extractIndex(int s) {
            return s & mask;
        }

        /**
         * Mask out the index, which is stored reversed,
         * flush-left, and the remaining bits are the
         * count.
         */
        final int extractCount(int s) {
            return s & ~mask;
        }
    }


    /**
     * Returns a bitmask with ones in the rightmost n positions,
     * where n is the number of bits needed to represent the
     * highest index seen.
     */
    private static final int indexMask(int n) {
        return Integer.reverse((Integer.highestOneBit(n-1) << 1) - 1);
    }

    private final int mask;
    private final Sync sync = new Sync();
    private final Map<G, LockImpl> map = new HashMap<G, LockImpl>();
    private final ConcurrentMap<Integer, Runnable> handlers =
        new ConcurrentHashMap<Integer, Runnable>();
}
