package com.quicklybly.itmo.salesanalytics;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Objects;
import org.apache.hadoop.io.Writable;

public class SalesWritable implements Writable {

    private BigDecimal revenue;
    private int quantity;

    public SalesWritable() {
        this.revenue = BigDecimal.ZERO;
    }

    public SalesWritable(BigDecimal revenue, int quantity) {
        this.revenue = revenue;
        this.quantity = quantity;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(revenue.toString());
        dataOutput.writeInt(quantity);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String revenueStr = dataInput.readUTF();
        this.revenue = new BigDecimal(revenueStr);
        this.quantity = dataInput.readInt();
    }

    public BigDecimal getRevenue() {
        return revenue;
    }

    public void setRevenue(BigDecimal revenue) {
        this.revenue = revenue;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;

        SalesWritable that = (SalesWritable) o;
        return quantity == that.quantity && Objects.equals(revenue, that.revenue);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(revenue);
        result = 31 * result + quantity;
        return result;
    }

    @Override
    public String toString() {
        return revenue.stripTrailingZeros().toPlainString() + "\t" + quantity;
    }
}
