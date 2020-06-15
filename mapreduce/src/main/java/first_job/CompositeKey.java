import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Composite key formed by a Natural Key (district) and Secondary Keys (count).
 * example of input 031-RECKLESS CONDUCT	1
 *
 */
public class CompositeKey implements WritableComparable<CompositeKey> {

    public String district;
    public Integer count;

    public CompositeKey() {
    }

    public CompositeKey(String district, Integer count) {
        super();
        this.set(district, count);
    }

    public void set(String district, Integer count) {
        this.district = (district == null) ? "" : district;
        this.count = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(district);
        out.writeInt(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        district = in.readUTF();
        count = in.readInt();
    }

    @Override
    public int compareTo(CompositeKey o) {
        int districtCmp = -1* district.toLowerCase().compareTo(o.district.toLowerCase());
        if (districtCmp != 0) {
            return districtCmp;
        } else {
            return -1* Integer.compare(count, o.count);
        }
    }

}