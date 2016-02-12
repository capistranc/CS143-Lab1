package simpledb;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private int fid;
    private File file;
    private TupleDesc tuple_desc;

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        fid = f.getAbsoluteFile().hashCode();
        tuple_desc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return fid;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tuple_desc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pgsize = Database.getBufferPool().PAGE_SIZE;
        byte[] bytes = new byte[pgsize];
        try {
            RandomAccessFile RAMfile = new RandomAccessFile(file.getAbsolutePath(), "r");
            if (BufferPool.getPageSize() * pid.pageNumber() > file.length()) {
                System.out.println("Trying to read another file");
                System.exit(1);
            }
            RAMfile.seek(pid.pageNumber()*pgsize);
            RAMfile.read(bytes);
            RAMfile.close();
            return new HeapPage((HeapPageId)pid, bytes);
        }
        catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
        // TODO: null ok to return?
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // TODO: need cast? or diff way to find byte count
        return (int) Math.ceil(file.length()/Database.getBufferPool().PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    private class HeapFileIterator implements DbFileIterator {
        private int curr_page_num;
        private HeapFile iter_heap;
        private TransactionId iter_tid;
        Iterator<Tuple> tuple_iterator;
        private final int INVALID = Integer.MAX_VALUE;

        public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
            iter_heap = heapFile;
            iter_tid = tid;
            curr_page_num = INVALID;
            //tuple_iterator = null;
        }

        public void open() throws DbException, TransactionAbortedException {
            curr_page_num = -1;
            tuple_iterator = null;
        }

        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (curr_page_num == INVALID)
                return false;
            Iterator<Tuple> temp = tuple_iterator;
            int orig_page_num = curr_page_num;
            if (temp != null && temp.hasNext())
                return true;
            else {
                curr_page_num++;
                while (curr_page_num < iter_heap.numPages()) {
                    try {
                        HeapPageId id = new HeapPageId(iter_heap.getId(), curr_page_num);
                        HeapPage p = (HeapPage) Database.getBufferPool().getPage(iter_tid, id, Permissions.READ_ONLY);
                        tuple_iterator = p.iterator();
                        if (tuple_iterator.hasNext()) {
                            tuple_iterator = temp;
                            curr_page_num = orig_page_num;
                            return true;
                        }
                    }
                    catch(DbException d) {
                        System.err.println(d.getMessage());
                    }
                    curr_page_num++;
                }
                tuple_iterator = temp;
                curr_page_num = orig_page_num;
                return false;
            }
       }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (curr_page_num == INVALID)
                throw new NoSuchElementException();
            if (tuple_iterator != null && tuple_iterator.hasNext())
                return tuple_iterator.next();
            else {
                curr_page_num++;
                while (curr_page_num < iter_heap.numPages()) {
                    try {
                        HeapPageId id = new HeapPageId(iter_heap.getId(), curr_page_num);
                        HeapPage p = (HeapPage) Database.getBufferPool().getPage(iter_tid, id, Permissions.READ_ONLY);
                        tuple_iterator = p.iterator();
                        if (tuple_iterator.hasNext()) {
                            return tuple_iterator.next();
                        }
                    }
                    catch(DbException d) {
                        System.err.println(d.getMessage());
                    }
                    curr_page_num++;
                }
                throw new NoSuchElementException();
            }
            /*
            // TODO: need exception here?
            if (curr_page_num == -1)
                throw new NoSuchElementException("Iterator not open yet");
            if (tuple_iterator.hasNext())
                return tuple_iterator.next();
            else if (hasNext()) {
                curr_page_num++;
                HeapPageId id = new HeapPageId(iter_heap.getId(), curr_page_num);
                // TODO: DbExceptions for getBufferPool
                try {
                    HeapPage p = (HeapPage) Database.getBufferPool().getPage(iter_tid, id, Permissions.READ_ONLY);
                    tuple_iterator = p.iterator();
                    return tuple_iterator.next();
                }
                catch (DbException e) {
                    System.out.println(e.getMessage());
                }
                return null;
            }
            else
                throw new NoSuchElementException();
                */
        }

        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        public void close() {
            tuple_iterator = null;
            curr_page_num = INVALID;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

}
