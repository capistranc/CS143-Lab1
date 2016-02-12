package simpledb;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private ArrayList<Field> tuple_fields;
    private TupleDesc tuple_desc;
    
    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        //TODO SOME CHECKING FOR td.num_fields
        if (td.numFields() < 1)
            throw new InvalidParameterException();
        
        tuple_fields = new ArrayList<Field>();
        tuple_desc = td;
        for (int i = 0; i < td.numFields(); i++){
            tuple_fields.add(null);
        }   
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
        // some code goes here
        return null;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
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
        //TODO ERROR CHECKING
        //if out of bounds, should already thrown exception
        if (i < 0 || i >= tuple_fields.size() ||
                !tuple_desc.getFieldType(i).equals(f.getType()))
            throw new InvalidParameterException();
        tuple_fields.set(i, f); 
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        //out of bounds handled by arraylist
        return tuple_fields.get(i);
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
        String tuples = "";
        for (int i = 0; i < this.getTupleDesc().numFields(); i++){
            if (!tuples.equals(""))
                tuples += "\t";
            tuples += getField(i).toString();
        }
        tuples += "\n";
        return tuples;
    }
   
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return tuple_fields.iterator();
    }
    
    /**
     * reset the TupleDesc of this tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        if (td.numFields() < 1)
            throw new InvalidParameterException();
        tuple_desc = td;
        tuple_fields = new ArrayList<Field>();
        for (int i = 0; i < td.numFields(); i++){
            tuple_fields.addAll(null);
        }   
    }
}