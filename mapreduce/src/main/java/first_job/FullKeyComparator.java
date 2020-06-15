import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FullKeyComparator extends WritableComparator {

    public FullKeyComparator() {
        super(CompositeKey.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {

        CompositeKey key1 = (CompositeKey) wc1;
        CompositeKey key2 = (CompositeKey) wc2;

        int districtCmp = key1.district.toLowerCase().compareTo(key2.district.toLowerCase());
        if (districtCmp != 0) {
            return districtCmp;
        } else {
            return -1 * Integer.compare(key1.count, key2.count);
        }

    }
}