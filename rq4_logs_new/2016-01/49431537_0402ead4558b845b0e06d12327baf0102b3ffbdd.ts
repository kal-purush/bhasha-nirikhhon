module AggregateFunctions {
    export function sum(columnName:string) : 
    (table:GroupedTable) => Number 
    {
        return function(table:GroupedTable) {
            let sum = 0;
            table.forEach((row) => sum += row[columnName]);
            return sum;
        }
    }
    
    export function avg(columnName:string) : 
    (table:GroupedTable) => Number 
    {
        return function(table:GroupedTable) {
            let sum = 0;
            table.forEach(row => sum += row[columnName]);
            return sum / table.length;
        }
    }
    
    export function count(table:GroupedTable) {
        return table.length;
    }
    
    export function first(table:GroupedTable) {
        return table[0];
    }
    
    export function last(table:GroupedTable) {
        return table[table.length - 1];
    }
}