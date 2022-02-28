package com.example.streams;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.reducing;

public class Trader {

    private final String name;
    private final String city;

    public Trader(String n, String c) {
        this.name = n;
        this.city = c;
    }

    public static void main(String[] args) {
        Trader raoul = new Trader("Raoul", "Cambridge");
        Trader mario = new Trader("Mario", "Milan");
        Trader alan = new Trader("Alan", "Cambridge");
        Trader brian = new Trader("Brian", "Cambridge");
        List<Trader> traders = Arrays.asList(raoul, mario, alan, brian);
        List<Transaction> transactions = Arrays.asList(
                new Transaction(brian, 2011, 300),
                new Transaction(raoul, 2012, 1000),
                new Transaction(raoul, 2011, 400),
                new Transaction(mario, 2012, 710),
                new Transaction(mario, 2012, 700),
                new Transaction(alan, 2012, 950)
        );
        //1. Find all transactions in the year 2011 and sort them by value (small to high).
        List<Transaction> transactionIn2011Sorted = transactions.stream()
                .filter(t -> t.getYear() == 2011)
                .sorted(Comparator.comparingInt(Transaction::getValue))
                .collect(Collectors.toList());
        System.out.println(transactionIn2011Sorted);

        //2.What are all the unique cities where the traders work?
        List<String> cities = traders.stream()
                .map(trader -> trader.getCity())
                .distinct()
                .collect(Collectors.toList());
        System.out.println(cities);

        //3. Find all traders from Cambridge and sort them by name
        List<Trader> cambridgeTraders = traders.stream()
                .filter(trader -> trader.getCity().equals("Cambridge"))
                .sorted(Comparator.comparing(Trader::getName))
                .collect(Collectors.toList());
        System.out.println(cambridgeTraders);

        //4. Return a string of all traders’ names sorted alphabetically
        Optional<String> traderNames = traders.stream()
                .sorted(Comparator.comparing(Trader::getName))
                .map(t -> t.getName())
                .reduce(String::concat);
        System.out.println(traderNames.get());

        //5. Are any traders based in Milan?
        boolean milanTraderExist = traders.stream().filter(t -> t.getCity().equals("Milan")).findFirst().isPresent();
        System.out.println(milanTraderExist);

        //6. Print the values of all transactions from the traders living in Cambridge.
        List<Integer> cambridgeTransactions = transactions.stream()
                .filter(transaction -> transaction.getTrader().getCity().equals("Cambridge"))
                .map(transaction -> transaction.getValue())
                .collect(Collectors.toList());
        System.out.println(cambridgeTransactions);

        //7. What’s the highest value of all the transactions
        Optional<Integer> highestTransaction = transactions.stream()
                .map(transaction -> transaction.getValue())
                .reduce((a, b) -> a > b ? a : b);
        System.out.println(highestTransaction);

        //8. Find the transaction with the smallest value.
        Optional<Transaction> min = transactions.stream().min(Comparator.comparingInt(Transaction::getValue));
        System.out.println(min);

        //9. Pythagoras triplet using IntStream
        Stream<int[]> stream = IntStream.rangeClosed(1, 100).boxed()
                .flatMap(a ->
                        IntStream.rangeClosed(a, 100)
                                .filter(b -> Math.sqrt(a * a + b * b) % 1 == 0)
                                .mapToObj(b ->
                                        new int[]{a, b, (int) Math.sqrt(a * a + b * b)})
                );

        //Find sum of all transactions using collect + reducing
        Integer sumOfTransaction = transactions.stream().collect(reducing(0, Transaction::getValue, Integer::sum));
        System.out.println(sumOfTransaction);

        //10. Grouping the collected data return the result
        Map<Integer, List<Transaction>> transactionsByYear = transactions.stream().collect(groupingBy(Transaction::getYear));

    }

    public String getName() {
        return this.name;
    }

    public String getCity() {
        return this.city;
    }

    public String toString() {
        return "Trader:" + this.name + " in " + this.city;
    }
}
