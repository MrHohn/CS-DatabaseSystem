package simpledb;

import java.io.*;
import java.util.concurrent.ConcurrentHashMap;
// import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool which check that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, excluding header. */
    public static final int PAGE_SIZE = 4096;
    public static final int DEFAULT_PAGES = 100;
    public static final int DEFAULT_POLICY = 0;
    public static final int LRU_POLICY = 1;
    public static final int MRU_POLICY = 2;

    int replace_policy = DEFAULT_POLICY;
    int _numhits=0;
    int _nummisses=0;
    
    // max num for pages
    private int maxPages;
    // real pool, store page contents along with PageIds
    private Cache pool;

    /**
     * Constructor.
     *
     * @param numPages number of pages in this buffer pool
     */
    public BufferPool(int numPages) {
        //IMPLEMENT THIS
        maxPages = numPages;
        pool = new Cache(numPages);
    }

    // Class for pool cache, constructed by double linked list and HashMap
    public class Cache {
        // double linked list to store page content
        // achieved in O(1) time complexity
        public class Node {
            // use PageId as the key
            private PageId key = null;
            // use page content as the value
            private Page val;
            private Node next = null, prev = null;
            // pinCount is not used in this assignment
            // because the test program never release page
            private int pinCount = 0;

            public Node(Page value, PageId k) {
                val = value;
                key = k;
            }

            public PageId getKey() {
                return key;
            }

            // remove current node
            public void remove() {
                prev.next = next;
                next.prev = prev;
            }

            // put current node before a specific node
            public void putBefore(Node h) {
                next = h;
                prev = h.prev;
                prev.next = this;
                h.prev = this;
            }
        }
        
        private Node headDum = new Node(null, null), tailDum = new Node(null, null);
        private int cap;
        // though synchronized funcions are already used
        // there might be multiple functions trying to access this hash map
        // hence use Concurrent version
        private ConcurrentHashMap<PageId, Node> map = new ConcurrentHashMap<PageId, Node>();

        public Cache(int capacity) {
            cap = capacity;
            headDum.next = tailDum;
            tailDum.prev = headDum;
        }
        
        public Page get(PageId key) {
            if (map.containsKey(key)) {
                Node cur = map.get(key);
                // disconnect it first
                cur.remove();
                // move to the head as the newest element
                cur.putBefore(headDum.next);
                return cur.val;
            }
            else {
                return null;
            }
        }

        public void add(PageId key, Page value) {
            //create new node and set up the key and value
            Node cur = new Node(value, key);
            // move to the head as the newest element
            cur.putBefore(headDum.next);
            // at last, put the new node into map
            map.put(key, cur);
        }

        // evict least recently used node and return its PageId
        public PageId evictLast() {
            // remove the oldest one, which is the tail
            // remove the key from map
            PageId removedPage = tailDum.prev.getKey();
            map.remove(removedPage);
            // delete the node from list
            tailDum.prev.remove();
            return removedPage;
        }

        // evict most recently used node
        public PageId evictFirst() {
            // remove the newest one, which is the head
            // remove the key from map
            PageId removedPage = headDum.next.getKey();
            map.remove(removedPage);
            // delete the node from list
            headDum.next.remove();
            return removedPage;
        }

        // get current size of the cache
        public int getSize() {
            return map.size();
        }

        // check if cache contains a PageId
        public boolean contains(PageId key) {
            return map.containsKey(key);
        }
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException, IOException {
        //IMPLEMENT THIS

        // check whether the requesting page is in pool
        if (pool.contains(pid)) {
            _numhits++;
            return pool.get(pid);
        }
        // not exist, read it from the disk and put it into cache
        else {
            _nummisses++;
            // read a page from a DbFile(HeapFile)
            Page currentPage = Database.getCatalog().getDbFile(pid.tableid()).readPage(pid);

            // if pool is full, evict one first
            if (pool.getSize() >= maxPages) {
                evictPage();
            }

            // put the new page into pool
            pool.add(pid, currentPage);

            return currentPage;   
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public synchronized void releasePage(TransactionId tid, PageId pid) {
        // no need to implement this
       
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public synchronized void transactionComplete(TransactionId tid) throws IOException {
       // no need to implement this
     }

    /** Return true if the specified transaction has a lock on the specified page */
    public  synchronized boolean holdsLock(TransactionId tid, PageId p) {
       // no need to implement this
         return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public  synchronized void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
       // no need to implement this
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to. May block if
     * the lock cannot be acquired.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public synchronized void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
       // no need to implement this

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is added to. May block if
     * the lock cannot be acquired.
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public synchronized void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
       // no need to implement this

    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
           // no need to implement this
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
     // no need to implement this
          }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private  synchronized void flushPage(PageId pid) throws IOException {
         // no need to implement this
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
             // no need to implement this
    }

    /**
     * Discards a page from the buffer pool. Return PageId of discarded page
     */
    private  synchronized PageId evictPage() throws DbException {
	    //IMPLEMENT THIS
        
        PageId removedPage = null;

        switch (replace_policy) {
            case DEFAULT_POLICY:
            case LRU_POLICY:
                removedPage = pool.evictLast();
                break;

            case MRU_POLICY:
                removedPage = pool.evictFirst();
                break;
        }

    	return removedPage;
    }

    public int getNumHits(){
	return _numhits;
    }
    public int getNumMisses(){
	return _nummisses;
    }
    
    public void setReplacePolicy(int replacement){
	this.replace_policy=replacement;
    }
}