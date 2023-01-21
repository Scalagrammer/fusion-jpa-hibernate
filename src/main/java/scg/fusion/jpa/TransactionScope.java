package scg.fusion.jpa;

import javax.persistence.EntityTransaction;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

final class TransactionScope implements AutoCloseable {

    private final EntityTransaction transaction;
    private final Runnable detachEntityManagerHook;

    private Exception cause;

    TransactionScope(Runnable detachEntityManagerHook, EntityTransaction transaction) {
        this.detachEntityManagerHook = detachEntityManagerHook;
        this.transaction = transaction;
        // activate transaction
        transaction.begin();
    }

    @Override
    public void close() throws Exception {
        try {
            if (transaction.isActive()) {

                if (isNull(cause)) {
                    transaction.commit();
                } else {

                    try {
                        transaction.rollback();
                    } catch (Exception suppressed) {
                        if (cause != suppressed) {
                            cause.addSuppressed(suppressed);
                        }
                    }

                    throw cause;

                }

            } else if (nonNull(cause)) {
                throw cause;
            }
        } finally {
            detachEntityManagerHook.run();
        }
    }

    Object initCause(Exception cause) {
        return this.cause = cause;
    }

}
