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
 * <p> Implementation of room synchronization as described in
 *  http://www-2.cs.cmu.edu/afs/cs/project/pscico/www/rooms.html
 *  and http://www.cs.brown.edu/courses/cs176/. </p>
 *
 * <p> There are no guarantees about acquisition order.
 * Conditions are not supported. The returned locks are
 * reentrant. </p>
 *
 * <p> Sample code for managing a regular (2 room) bathroom
 * under a policy that dictates that males who cannot acquire
 * the lock after a minute must use alternative facilities and
 * females who cannot acquire the lock must block indefinitely
 * in the hallway: </p>
 *
 * <pre>
 * abstract class BathroomManager {
 *     enum Gender{ MALE, FEMALE }
 *     private final RoomSynchronizer<Gender> wc =
 *         new RoomSynchronizer<Gender>(Gender.values());
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
 *     abstract void useBathroom(R g);
 *     abstract void useAlternativeFacilities();
 * }
 * </pre>
 *
 * <h2> Implementation limitations </h2>
 *
 * <p> This implementation uses a single int to encode two
 * pieces of information: the origin-1 index of the room
 * that is currently in use, or 0 if no room is in use; and
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
public final class RoomSynchronizer<R> {

    /**
     * Creates a new instance of RoomSynchronizer using the
     * given rooms.
     */
    public RoomSynchronizer(R[] rooms) {
        int n = 1;
        for (R room : rooms)
            this.rooms.put(room, new LockImpl(n++));
        this.mask = indexMask(n);
    }

    /**
     * Creates a new instance of RoomSynchronizer using the
     * given rooms.
     */
    public RoomSynchronizer(Collection<R> rooms) {
        int n = 1;
        for (R room : rooms)
            this.rooms.put(room, new LockImpl(n++));
        this.mask = indexMask(n);
    }

    /**
     * Returns the lock associated with the given room.
     */
    public Lock lockFor(R room) {
        Lock result = rooms.get(room);
        if (result == null)
            throw new IllegalArgumentException();
        return result;
    }


    /**
     * All lock behavior is defined in terms of a single
     * AQS sync object. Locks pass their index as the argument
     * value to acquire and release methods.
     */
    private class LockImpl implements Lock {
        public void lock() { sync.acquireShared(index); }
        public void unlock() { sync.releaseShared(index); }
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
        public Condition newCondition() {
            throw new UnsupportedOperationException();
        }

        LockImpl(int index) { this.index = Integer.reverse(index); }
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
        final int combineIndexAndCount(int index, int count) { return count | index; }
        final int extractIndex(int s) { return s & mask; }
        final int extractCount(int s) { return s & ~mask; }
    }


    /**
     * Returns a bitmask with ones in the leftmost n positions,
     * where n is the number of bits needed to represent the
     * highest index seen.
     */
    private static final int indexMask(int n) {
        return Integer.reverse((Integer.highestOneBit(n-1) << 1) - 1);
    }

    private final int mask;
    private final Sync sync = new Sync();
    private final Map<R, LockImpl> rooms = new HashMap<R, LockImpl>();
}
