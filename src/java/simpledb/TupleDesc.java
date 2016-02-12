package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        
        // some code goes here
        return schema.iterator();
    }
    
    
    private static final long serialVersionUID = 1L;
        
    private ArrayList<TDItem> schema;
    private int tuple_size;
    
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        //TODO error checking for dumb parameters
        tuple_size = 0;
        schema = new ArrayList<TDItem>();
        
        for (int x = 0; x < typeAr.length; x++){
            schema.add(new TDItem(typeAr[x], fieldAr[x]));
            tuple_size += typeAr[x].getLen();
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        //TODO error checking for dumb parameters
        tuple_size = 0;
        schema = new ArrayList<TDItem>();

        for (int x = 0; x < typeAr.length; x++){
            schema.add(new TDItem(typeAr[x], ""));
            tuple_size += typeAr[x].getLen();
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return schema.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || i >= schema.size())
            throw new NoSuchElementException();
        return schema.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < 0 || i >= schema.size())
            throw new NoSuchElementException();
        return schema.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if (name == null)
            throw new NoSuchElementException();
        for (int i = 0; i < schema.size(); i++){
            if (schema.get(i).fieldName == null)
                continue;
            if (schema.get(i).fieldName.equals(name)){
                return i;
            }
        }
        throw new NoSuchElementException();
    
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        return tuple_size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        Type[] types = {};
        String[] strings = {};
        TupleDesc merged = new TupleDesc(types, strings);
        for (int i = 0; i < td1.numFields(); i++){
            merged.insertField(td1.getFieldType(i), td1.getFieldName(i));
        }
        for (int i = 0; i < td2.numFields(); i++){
            merged.insertField(td2.getFieldType(i), td2.getFieldName(i));
        }
        
        return merged;
    }
    
    private void insertField(Type n_field, String n_name){
        TDItem n_item = new TDItem(n_field, n_name);
        schema.add(n_item);
        tuple_size += n_field.getLen();
        
    }
    
    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (o == null)
            return false;
        if (o instanceof TupleDesc) {
            TupleDesc tuple_o = ((TupleDesc) o);
            if (tuple_o.getSize() == this.getSize()) {
                for (int i = 0; i < this.numFields(); i++) {
                    if (!tuple_o.getFieldType(i).equals(this.getFieldType(i))) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String desc = "";
        for (int i = 0; i < this.numFields();i++){
            if (!desc.equals(""))
                desc += ",";
            desc += String.format("%s(%s)", this.getFieldType(i).toString(), this.getFieldName(i));
            
        }
        return desc;
    }
}