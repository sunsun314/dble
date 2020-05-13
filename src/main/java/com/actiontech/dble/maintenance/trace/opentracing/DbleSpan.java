package com.actiontech.dble.maintenance.trace.opentracing;

import io.opentracing.Scope;
import io.opentracing.Span;

/**
 * Created by szf on 2020/5/13.
 */
public class DbleSpan implements Cloneable {

    private final Span span;
    private final Scope scope;

    public DbleSpan(Span span, Scope scope) {
        this.scope = scope;
        this.span = span;
    }

    public void finish() {
        if (scope != null) {
            scope.close();
        }
        span.finish();
    }

    public void close() {
        this.finish();
    }
}
