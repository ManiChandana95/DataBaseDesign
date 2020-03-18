package Model;

public class Results {
    public int rowsAffected;
    public boolean isInternal = false;

    public Results(int rowsAffected) {
        this.rowsAffected = rowsAffected;
    }

    public Results(int rowsAffected, boolean isInternal) {
        this.rowsAffected = rowsAffected;
        this.isInternal = isInternal;
    }

    public void Display() {
        if(this.isInternal) return;
        System.out.println("Query Successful");
        System.out.println(String.format("%d rows affected", this.rowsAffected));
        System.out.println();
    }
}
