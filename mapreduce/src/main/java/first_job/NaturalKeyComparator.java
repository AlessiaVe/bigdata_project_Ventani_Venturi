import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/**
 * class for comporator the natural key
 */
public class NaturalKeyComparator extends WritableComparator {

    public NaturalKeyComparator() {
        super(CompositeKey.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {

        CompositeKey key1 = (CompositeKey) wc1;
        CompositeKey key2 = (CompositeKey) wc2;
        return key1.district.compareTo(key2.district);

    }

}