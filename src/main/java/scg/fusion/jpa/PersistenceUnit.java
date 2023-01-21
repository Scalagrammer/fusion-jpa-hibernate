package scg.fusion.jpa;

import scg.fusion.Environment;
import scg.fusion.annotation.Around;
import scg.fusion.annotation.Factory;
import scg.fusion.annotation.UtilizeBy;
import scg.fusion.aop.JoinPoint;

public final class PersistenceUnit {

    public static final String PERSISTENCE_UNIT_NAME_PROPERTY_NAME = "fusion.persistence.unit.name";

    private final TransactionService service;

    private PersistenceUnit(TransactionService service) {
        this.service = service;
    }

    @Around("@execution(scg.fusion.jpa.annotations.Transactional)")
    private Object aroundTransactional(JoinPoint joint) throws Throwable {
        if (service.isWithinTransaction()) {
            return joint.proceed();
        } else {
            return proceedTransactional(joint);
        }
    }

    private Object proceedTransactional(JoinPoint joint) throws Throwable {
        try (TransactionScope scope = service.begin()) {
            try {
                return joint.proceed();
            } catch (Exception cause) {
                return scope.initCause(cause); // never return from here
            }
        }
    }

    @Factory
    @UtilizeBy("destroy")
    private static EntityManagerImpl entityManager(Environment environment) {
        return new EntityManagerImpl(environment.getProperty(PERSISTENCE_UNIT_NAME_PROPERTY_NAME));
    }

}
