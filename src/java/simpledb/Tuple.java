package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.*;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private ArrayList<Field> tuple_list;
    private TupleDesc tuple_desc;
    private RecordId rec_id;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        int len = td.getNumFields();

        if (len < 1)
            throw new IllegalArgumentException("Invalid Field Length");

        tuple_list = new ArrayList<Field>();
        tuple_desc = td;

        for (int i = 0; i < len; i++)
            tuple_list.add(null);
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return tuple_desc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return rec_id;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        rec_id = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        if (i < 0 || i >= tuple_list.size())
            throw new IllegalArgumentException("Invalid index");

        tuple_list.set(i, f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        if (i < 0 || i >= tuple_list.size())
            throw new IllegalArgumentException("Invalid index");

        return tuple_list.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        String ret = "";

        for (ListIterator<Field> it = this.fields(); it.hasNext(); )
        {
            if (!it.next().toString().equals(""))
                ret += "\t";
            ret += it.previous().toString();
        }
        ret += "\n";

        return ret;
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public ListIterator<Field> fields()
    {
        return tuple_list.listIterator();
    }
    
    /**
     * reset the TupleDesc of this tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        int len = td.getNumFields();

        if (len < 1)
            throw new IllegalArgumentException("Invalid Field Length");

        tuple_desc = td;
        tuple_list = new ArrayList<Field>();

        for (int i = 0; i < len; i++)
            tuple_list.add(null);
    }
}
