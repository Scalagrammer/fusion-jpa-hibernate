package scg.fusion.jpa;

import javax.persistence.*;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaDelete;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.CriteriaUpdate;
import javax.persistence.metamodel.Metamodel;

import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.lang.ThreadLocal.withInitial;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

final class EntityManagerImpl implements EntityManager, TransactionService {

    private volatile EntityManagerFactory factory;

    private final String persistenceUnitName;

    private final ThreadLocal<Stack<EntityManager>> stack = withInitial(Stack::new);

    EntityManagerImpl(String persistenceUnitName) {
        this.persistenceUnitName = persistenceUnitName;
    }

    @Override
    public boolean isWithinTransaction() {

        Stack<EntityManager> state = getState();

        return !state.empty();

    }

    @Override
    public TransactionScope begin() {

        EntityManagerFactory factory = getFactory();

        EntityManager entityManager = register(factory.createEntityManager());

        return new TransactionScope(unregister(entityManager), entityManager.getTransaction());

    }

    @Override
    public void persist(Object entity) {
        acceptState(entityManager -> entityManager.persist(entity));
    }

    @Override
    public <T> T merge(T entity) {
        return withinState(entityManager -> entityManager.merge(entity));
    }

    @Override
    public void remove(Object entity) {
        acceptState(entityManager -> entityManager.remove(entity));
    }

    @Override
    public <T> T find(Class<T> entityClass, Object primaryKey) {
        return withinState(entityManager -> entityManager.find(entityClass, primaryKey));
    }

    @Override
    public <T> T find(Class<T> entityClass, Object primaryKey, Map<String, Object> properties) {
        return withinState(entityManager -> entityManager.find(entityClass, primaryKey, properties));
    }

    @Override
    public <T> T find(Class<T> entityClass, Object primaryKey, LockModeType lockMode) {
        return withinState(entityManager -> entityManager.find(entityClass, primaryKey, lockMode));
    }

    @Override
    public <T> T find(Class<T> entityClass, Object primaryKey, LockModeType lockMode, Map<String, Object> properties) {
        return withinState(entityManager -> entityManager.find(entityClass, primaryKey, lockMode, properties));
    }

    @Override
    public <T> T getReference(Class<T> entityClass, Object primaryKey) {
        return withinState(entityManager -> entityManager.getReference(entityClass, primaryKey));
    }

    @Override
    public void flush() {
        acceptState(EntityManager::flush);
    }

    @Override
    public void setFlushMode(FlushModeType flushMode) {
        acceptState(entityManager -> entityManager.setFlushMode(flushMode));
    }

    @Override
    public FlushModeType getFlushMode() {
        return withinState(EntityManager::getFlushMode);
    }

    @Override
    public void lock(Object entity, LockModeType lockMode) {
        acceptState(entityManager -> entityManager.lock(entity, lockMode));
    }

    @Override
    public void lock(Object entity, LockModeType lockMode, Map<String, Object> properties) {
        acceptState(entityManager -> entityManager.lock(entity, lockMode, properties));
    }

    @Override
    public void refresh(Object entity) {
        acceptState(entityManager -> entityManager.refresh(entity));
    }

    @Override
    public void refresh(Object entity, Map<String, Object> properties) {
        acceptState(entityManager -> entityManager.refresh(entity, properties));
    }

    @Override
    public void refresh(Object entity, LockModeType lockMode) {
        acceptState(entityManager -> entityManager.refresh(entity, lockMode));
    }

    @Override
    public void refresh(Object entity, LockModeType lockMode, Map<String, Object> properties) {
        acceptState(entityManager -> entityManager.refresh(entity, lockMode, properties));
    }

    @Override
    public void clear() {
        acceptState(EntityManager::clear);
    }

    @Override
    public void detach(Object entity) {
        acceptState(entityManager -> entityManager.detach(entity));
    }

    @Override
    public boolean contains(Object entity) {
        return withinState(entityManager -> entityManager.contains(entity));
    }

    @Override
    public LockModeType getLockMode(Object entity) {
        return withinState(entityManager -> entityManager.getLockMode(entity));
    }

    @Override
    public void setProperty(String propertyName, Object value) {
        acceptState(entityManager -> entityManager.setProperty(propertyName, value));
    }

    @Override
    public Map<String, Object> getProperties() {
        return withinState(EntityManager::getProperties);
    }

    @Override
    public Query createQuery(String qlString) {
        return withinState(entityManager -> entityManager.createQuery(qlString));
    }

    @Override
    public <T> TypedQuery<T> createQuery(CriteriaQuery<T> criteriaQuery) {
        return withinState(entityManager -> entityManager.createQuery(criteriaQuery));
    }

    @Override
    public Query createQuery(CriteriaUpdate updateQuery) {
        return withinState(entityManager -> entityManager.createQuery(updateQuery));
    }

    @Override
    public Query createQuery(CriteriaDelete deleteQuery) {
        return withinState(entityManager -> entityManager.createQuery(deleteQuery));
    }

    @Override
    public <T> TypedQuery<T> createQuery(String qlString, Class<T> resultClass) {
        return withinState(entityManager -> entityManager.createQuery(qlString, resultClass));
    }

    @Override
    public Query createNamedQuery(String name) {
        return withinState(entityManager -> entityManager.createNamedQuery(name));
    }

    @Override
    public <T> TypedQuery<T> createNamedQuery(String name, Class<T> resultClass) {
        return withinState(entityManager -> entityManager.createNamedQuery(name, resultClass));
    }

    @Override
    public Query createNativeQuery(String sqlString) {
        return withinState(entityManager -> entityManager.createNativeQuery(sqlString));
    }

    @Override
    public Query createNativeQuery(String sqlString, Class resultClass) {
        return withinState(entityManager -> entityManager.createNativeQuery(sqlString, resultClass));
    }

    @Override
    public Query createNativeQuery(String sqlString, String resultSetMapping) {
        return withinState(entityManager -> entityManager.createNativeQuery(sqlString, resultSetMapping));
    }

    @Override
    public StoredProcedureQuery createNamedStoredProcedureQuery(String name) {
        return withinState(entityManager -> entityManager.createNamedStoredProcedureQuery(name));
    }

    @Override
    public StoredProcedureQuery createStoredProcedureQuery(String procedureName) {
        return withinState(entityManager -> entityManager.createStoredProcedureQuery(procedureName));
    }

    @Override
    public StoredProcedureQuery createStoredProcedureQuery(String procedureName, Class... resultClasses) {
        return withinState(entityManager -> entityManager.createStoredProcedureQuery(procedureName, resultClasses));
    }

    @Override
    public StoredProcedureQuery createStoredProcedureQuery(String procedureName, String... resultSetMappings) {
        return withinState(entityManager -> entityManager.createStoredProcedureQuery(procedureName, resultSetMappings));
    }

    @Override
    public void joinTransaction() {
        acceptState(EntityManager::joinTransaction);
    }

    @Override
    public boolean isJoinedToTransaction() {
        return withinState(EntityManager::isJoinedToTransaction);
    }

    @Override
    public <T> T unwrap(Class<T> cls) {
        return withinState(entityManager -> entityManager.unwrap(cls));
    }

    @Override
    public Object getDelegate() {
        return withinState(EntityManager::getDelegate);
    }

    @Override
    public void close() {
        acceptState(EntityManager::close);
    }

    @Override
    public boolean isOpen() {
        return withinState(EntityManager::isOpen);
    }

    @Override
    public EntityTransaction getTransaction() {
        return withinState(EntityManager::getTransaction);
    }

    @Override
    public EntityManagerFactory getEntityManagerFactory() {
        return withinState(EntityManager::getEntityManagerFactory);
    }

    @Override
    public CriteriaBuilder getCriteriaBuilder() {
        return withinState(EntityManager::getCriteriaBuilder);
    }

    @Override
    public Metamodel getMetamodel() {
        return withinState(EntityManager::getMetamodel);
    }

    @Override
    public <T> EntityGraph<T> createEntityGraph(Class<T> rootType) {
        return withinState(entityManager -> entityManager.createEntityGraph(rootType));
    }

    @Override
    public EntityGraph<?> createEntityGraph(String graphName) {
        return withinState(entityManager -> entityManager.createEntityGraph(graphName));
    }

    @Override
    public EntityGraph<?> getEntityGraph(String graphName) {
        return withinState(entityManager -> entityManager.getEntityGraph(graphName));
    }

    @Override
    public <T> List<EntityGraph<? super T>> getEntityGraphs(Class<T> entityClass) {
        return withinState(entityManager -> entityManager.getEntityGraphs(entityClass));
    }

    private Runnable unregister(EntityManager entityManager) {
        return () -> {

            Stack<EntityManager> state = getState();

            if (!state.isEmpty() && entityManager != state.pop()) {
                entityManager.close();
            }
        };
    }

    private EntityManager register(EntityManager entityManager) {

        Stack<EntityManager> state = getState();

        return state.push(entityManager);

    }

    private void acceptState(Consumer<EntityManager> within) {
        within.accept(lookup());
    }

    private  <R> R withinState(Function<EntityManager, R> within) {
        return within.apply(lookup());
    }

    private EntityManager lookup() {

        Stack<EntityManager> state = getState();

        if (state.isEmpty()) {
            throw new RuntimeException("Cannot access to persistence unit from non-transactional scope");
        } else {
            return state.peek();
        }
    }

    private Stack<EntityManager> getState() {
        return stack.get();
    }

    private EntityManagerFactory getFactory() {

        if (isNull(factory)) {
            synchronized (this) {
                if (isNull(factory)) {
                    factory = Persistence.createEntityManagerFactory(persistenceUnitName);
                }
            }
        }

        return factory;

    }

    private void destroy() {
        if (nonNull(factory)) {
            factory.close();
        }
    }

}
