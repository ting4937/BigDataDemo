package cn.cloud.bigdata.flowcount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Description:
 *
 * @author: Cloud
 * Date: 2018-06-22
 * Time: 9:36
 */
public class FlowBean implements Writable {
    private long upflow;
    private long downflow;
    private long totalflow;

    public FlowBean() {

    }

    public FlowBean(long upflow, long downflow) {
        this.upflow = upflow;
        this.downflow = downflow;
        totalflow = upflow + downflow;
    }

    public long getUpflow() {
        return upflow;
    }

    public void setUpflow(long upflow) {
        this.upflow = upflow;
    }

    public long getDownflow() {
        return downflow;
    }

    public void setDownflow(long downflow) {
        this.downflow = downflow;
    }

    public long getTotalflow() {
        return totalflow;
    }

    public void setTotalflow(long totalflow) {
        this.totalflow = totalflow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upflow);
        dataOutput.writeLong(downflow);
        dataOutput.writeLong(totalflow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upflow = dataInput.readLong();
        downflow = dataInput.readLong();
        totalflow = dataInput.readLong();
    }

    @Override
    public String toString() {
        return upflow + "\t" + downflow + "\t" + totalflow;
    }
}
