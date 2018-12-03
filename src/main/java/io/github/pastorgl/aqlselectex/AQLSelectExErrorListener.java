package io.github.pastorgl.aqlselectex;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

public class AQLSelectExErrorListener extends BaseErrorListener {
    private boolean hasError;
    private Exception exception;

    @Override
    public void syntaxError(final Recognizer<?, ?> recognizer, final Object offendingSymbol, final int line, final int charPositionInLine, final String msg, final RecognitionException e) {
        hasError = true;

        final StringBuilder strBuffer = new StringBuilder();
        strBuffer.append("=> line ").append(line).append(" : ").append(msg);
        if (e != null) {
            if (e.getMessage() != null) {
                strBuffer.append(e.getMessage());
            }
            if (e.getCtx() != null) {
                strBuffer.append("Context : ").append(e.getCtx());
            }
        }

        exception = new Exception(strBuffer.toString(), e);
    }

    public boolean hasError() {
        return hasError;
    }

    public Exception exception() {
        return exception;
    }
}
