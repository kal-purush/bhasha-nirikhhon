package price.weston.amazeing;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.*;

/**
 * Created by wprice on 4/21/17.
 */
public class CellBlock {

    private int row;
    private int column;
    private Set<CellBlock> connections = new HashSet<>();

    public CellBlock(int row, int column) {
        this.row = row;
        this.column = column;

    }

    public int getRow() {
        return row;
    }

    public int getColumn() {
        return column;
    }

    public void addConnection(CellBlock cellBlock) {

        if(!connections.contains(cellBlock)) {
            connections.add(cellBlock);
            cellBlock.addConnection(this);
        }

    }

    @Override
    public boolean equals(Object obj) {

        return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getRow()).append(getColumn()).hashCode();
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

}