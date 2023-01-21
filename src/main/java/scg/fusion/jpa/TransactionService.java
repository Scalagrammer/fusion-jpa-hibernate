package scg.fusion.jpa;

interface TransactionService {

    TransactionScope begin();

    boolean isWithinTransaction();

}
