package com.example;

public class CustomerTransaction {
    int count;
    int balance;
    String time;

    public CustomerTransaction(){

    }

    public CustomerTransaction(int count, int balance, String time) {
        this.count = count;
        this.balance = balance;
        this.time = time;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getBalance() {
        return balance;
    }

    public void setBalance(int balance) {
        this.balance = balance;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }
}
