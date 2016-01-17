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
    public static class TDItem implements Serializable 
    {

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

    private ArrayList<TDItem> td_list;
    private int tuple_size;
    private int num_fields;
    private static final long serialVersionUID = 1L;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return td_list.listIterator();
    }



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
    public TupleDesc(Type[] typeAr, String[] fieldAr) 
    {
        num_fields = typeAr.length;
        tuple_size = 0;

        td_list = new ArrayList<TDItem>();


        for (int i = 0; i < num_fields; i++)
        {
            TDItem temp = new TDItem(typeAr[i], fieldAr[i]);
            td_list.add(temp);

            tuple_size += typeAr[i].getLen();
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
    public TupleDesc(Type[] typeAr) 
    {
        num_fields = typeAr.length;
        tuple_size = 0;

        td_list = new ArrayList<TDItem>();


        for (int i = 0; i < num_fields; i++)
        {
            TDItem temp = new TDItem(typeAr[i], "");
            td_list.add(temp);

            tuple_size += typeAr[i].getLen();
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int getNumFields() {
        return num_fields;
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
        if (i < 0 || i >= num_fields)
            throw new NoSuchElementException();

        return td_list.get(i).fieldName;
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
        if (i < 0 || i >= num_fields)
            throw new NoSuchElementException();

        return td_list.get(i).fieldType;
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

        for (int i = 0; i < num_fields; i++) 
        {
            if (td_list.get(i).fieldName.equals(name))
                return i;
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

    public ArrayList<TDItem> getTDArrayList() {
        return td_list;
    }


    //Helper function for merge, essentially clones a TupleDesc
    public TupleDesc(int n_fields, int t_size, ArrayList<TDItem> new_list) 
    {
        num_fields = 0;
        tuple_size = 0;
        td_list = new_list;
    }

    /**
     * Merge two TupleDescs into one, with td1.getNumFields + td2.getNumFields fields,
     * with the first td1.getNumFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) 
    {
        int m_fields = td1.getNumFields() + td2.getNumFields();
        int m_size = td1.getSize() + td2.getSize();

        ArrayList<TDItem> m_list = td1.getTDArrayList();
        m_list.addAll(td2.getTDArrayList());

        TupleDesc merged_td = new TupleDesc(m_fields, m_size, m_list);

        return merged_td;
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
        else if (o instanceof TupleDesc) 
        {
            TupleDesc tdo = (TupleDesc) o;
            int len_fields = this.getNumFields();

            if (tdo.getSize() == this.getSize() && tdo.getNumFields() == len_fields)
            {
                for (int i = 0; i < len_fields; i++) 
                {
                    if (!tdo.getFieldType(i).equals(this.getFieldType(i)))
                        return false;
                
                }
                return true; //If this loop completes
            }
        }
        return false; //if o is not an instance of TupleDesc
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
    public String toString() 
    {
        String ret = "";
        
        int len = this.getNumFields();
        for  (int i = 0; i < len; i++) 
        {
            if (!ret.equals(""))
                ret += ",";

            ret += String.format("%s(%s)", this.getFieldType(i).toString(), this.getFieldName(i));
        }

        return ret;
    }
}
