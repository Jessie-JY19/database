package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        if (readonly) {
            if (lockType == LockType.X || lockType == LockType.IX)
                throw new UnsupportedOperationException("X/IX on read-only");
        }
        if (hasSIXAncestor(transaction) && (lockType == LockType.S || lockType == LockType.IS))
            throw new InvalidLockException("S/IS after SIX");
        LockType effective = getActualLockType(transaction);
        if (!LockType.canBeParentLock(effective, lockType)){
            if (effective != lockType.NL)
                throw new InvalidLockException("cannot be parent lock");
        }
        lockman.acquire(transaction, name, lockType);
        numChildLocksAdd(transaction, parent);
    }
    private void numChildLocksAdd(TransactionContext trans, LockContext cur) {
        if (cur == null) return;
        int childLocks = cur.numChildLocks.getOrDefault(trans.getTransNum(), 0);
        cur.numChildLocks.put(trans.getTransNum(), childLocks + 1);
        numChildLocksAdd(trans, cur.parent);
    }
    private void numChildLocksDel(TransactionContext trans, LockContext cur) {
        if (cur == null) return;
        int childLocks = cur.numChildLocks.getOrDefault(trans.getTransNum(), 0);
        //if (childLocks == 0) return;
        childLocks = childLocks == 0 ? 0 : childLocks - 1;
        cur.numChildLocks.put(trans.getTransNum(), childLocks);
        numChildLocksDel(trans, cur.parent);
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if (readonly) throw new UnsupportedOperationException("read only");
        if (lockman.getLockType(transaction, name) == LockType.NL)
            throw new NoLockHeldException("no lock to release");
        int childLocks = numChildLocks.getOrDefault(transaction.getTransNum(), 0);
        if (childLocks != 0)
            throw new InvalidLockException("this lock has child locks");
        lockman.release(transaction, name);
        numChildLocksDel(transaction, parent);
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if (readonly) throw new UnsupportedOperationException("read only");

        LockType curLockType = lockman.getLockType(transaction, name);

        if (curLockType == newLockType)
            throw new DuplicateLockRequestException("already has new lock type");
        if (curLockType == LockType.NL)
            throw new NoLockHeldException("no lock held");

        List<ResourceName> sisDesc = sisDescendants(transaction);
        boolean hasSIXAn = hasSIXAncestor(transaction);
        boolean sub = LockType.substitutable(newLockType, curLockType);

        if (newLockType != LockType.SIX) {
            if (!sub)
                throw new InvalidLockException("not substitutable");
            lockman.promote(transaction, name, newLockType);
        } else {
            boolean curISXS = curLockType == LockType.S || curLockType == LockType.IS || curLockType == LockType.IX;
            if (!sub && !curISXS)
                throw new InvalidLockException("not substitutable");
            if (hasSIXAn) throw new InvalidLockException("has SIX ancestor");

            sisDesc.add(name);
            lockman.acquireAndRelease(transaction, name, newLockType, sisDesc);
            for (ResourceName res: sisDesc) {
                if (res == name) continue;
                LockContext loc = fromResourceName(lockman, res);
                loc.numChildLocksDel(transaction, loc.parent);
            }
        }
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        if (readonly) throw new UnsupportedOperationException("read only");
        LockType curLockType = lockman.getLockType(transaction, name);
        if (curLockType == LockType.X || curLockType == LockType.S)
            return;
        if (curLockType == LockType.NL)
            throw new NoLockHeldException("no lock held");
        List<Lock> transLocks = lockman.getLocks(transaction);
        List<LockContext> descCon = new ArrayList<>();
        List<ResourceName> descRes = new ArrayList<>();
        descCon.add(this);
        descRes.add(this.name);
        for (Lock l : transLocks) {
            LockContext con = fromResourceName(lockman, l.name);
            if (isAncestor(this, con)){
                descCon.add(con);
                descRes.add(con.name);
            }
        }
        if (curLockType == LockType.IX || curLockType == LockType.SIX) {
            lockman.acquireAndRelease(transaction, name, LockType.X, descRes);
            for (LockContext con : descCon) {
                if (con == this) continue;
                numChildLocksDel(transaction, con.parent);
            }
        }
        if (curLockType == LockType.IS) {
            lockman.acquireAndRelease(transaction, name, LockType.S, descRes);
            for (LockContext con : descCon) {
                if (con == this) continue;
                numChildLocksDel(transaction, con.parent);
            }
        }
        return;
    }


    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        return lockman.getLockType(transaction, name);
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        /**if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        LockType exLockType = getExplicitLockType(transaction);
        if (exLockType != LockType.NL)
            return exLockType;
        LockType ansLockType = LockType.NL;
        LockContext cur = parent;
        while (cur != null) {
            ansLockType = cur.getExplicitLockType(transaction);
            if (ansLockType != LockType.NL)
                break;
            cur = cur.parent;
        }
        if (ansLockType == LockType.IS || ansLockType == LockType.IX)
            return LockType.NL;
        if (ansLockType == LockType.SIX)
            return LockType.S;
        return ansLockType; */

        LockType explicit = getExplicitLockType(transaction);
        if (explicit == LockType.X || explicit == LockType.S || explicit == LockType.IS)
            return explicit;

        LockContext cur = parent;
        boolean hasSIX = false;
        LockType ans;
        while (cur != null) {
            ans = cur.getExplicitLockType(transaction);
            if (ans == LockType.S || ans == LockType.X)
                return ans;
            if (ans == LockType.SIX) {
                hasSIX = true;
                break;
            }
            cur = cur.parent;
        }
        if (hasSIX && explicit == LockType.IX)
            return LockType.SIX;
        if (hasSIX)
            return LockType.S;
        return LockType.NL;
    }

    public LockType getActualLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        LockType exLockType = getExplicitLockType(transaction);
        if (exLockType != LockType.NL)
            return exLockType;
        LockType ansLockType = LockType.NL;
        LockContext cur = parent;
        while (cur != null) {
            ansLockType = cur.getExplicitLockType(transaction);
            if (ansLockType != LockType.NL)
                break;
            cur = cur.parent;
        }
        return ansLockType;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        LockContext cur = parent;
        while (cur != null) {
            LockType curLockType = lockman.getLockType(transaction, cur.name);
            if (curLockType == LockType.SIX)
                return true;
            cur = cur.parent;
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<Lock> manLocks = lockman.getLocks(transaction);
        List<ResourceName> resourceNames = new ArrayList<>();
        for (Lock l : manLocks) {
            if (l.lockType == LockType.S || l.lockType == LockType.IS) {
                LockContext lc = fromResourceName(lockman, l.name);
                if(isAncestor(this, lc))
                    resourceNames.add(l.name);
            }
        }
        return resourceNames;
    }
    private boolean isAncestor(LockContext high, LockContext low) {
        if (low == null) return false;
        if (high == null) return true;
        LockContext cur = low.parent;
        while (cur != null) {
            if (cur.equals(high))
                return true;
            cur = cur.parent;
        }
        return false;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

