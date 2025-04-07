package org.apache.sling.event.impl.support;

public class PropertyNameAndOperationExtractor {
    public PropertyNameAndOperation extractPropertyNameAndOperation(String key) {
        final char firstChar = key.length() > 0 ? key.charAt(0) : 0;
        final String propName;
        final Operation op;

        if ( firstChar == '=' ) {
            propName = key.substring(1);
            op  = Operation.EQUALS;
        } else if ( firstChar == '<' ) {
            final char secondChar = key.length() > 1 ? key.charAt(1) : 0;
            if ( secondChar == '=' ) {
                op = Operation.LESS_OR_EQUALS;
                propName = key.substring(2);
            } else {
                op = Operation.LESS;
                propName = key.substring(1);
            }
        } else if ( firstChar == '>' ) {
            final char secondChar = key.length() > 1 ? key.charAt(1) : 0;
            if ( secondChar == '=' ) {
                op = Operation.GREATER_OR_EQUALS;
                propName = key.substring(2);
            } else {
                op = Operation.GREATER;
                propName = key.substring(1);
            }
        } else {
            propName = key;
            op  = Operation.EQUALS;
        }

        return new PropertyNameAndOperation(propName, op);
    }
}
