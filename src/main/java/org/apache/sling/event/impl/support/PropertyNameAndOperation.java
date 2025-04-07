package org.apache.sling.event.impl.support;

public class PropertyNameAndOperation {
    private final String propertyName;
    private final Operation operation;

    public PropertyNameAndOperation(String propertyName, Operation operation) {
        this.propertyName = propertyName;
        this.operation = operation;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public Operation getOperation() {
        return operation;
    }
}
