package com.example.NYCTrafficAnalysisApp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;


/**
 * Created by aayushi on 7/1/16.
 */

/**
 * Writes tuples to stdout of the container.
 * <p>
 * Mainly to be used for debugging. Users should be careful to not have this node listen to a high throughput stream<br>
 * <br>
 * </p>
 * @displayName Console Output
 * @category Output
 * @tags output operator
 *
 * @since 0.3.2
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
@Stateless
public class ConsoleOutputOperator extends BaseOperator
{
    private static final Logger logger = LoggerFactory.getLogger(ConsoleOutputOperator.class);

    /**
     * This is the input port which receives the tuples that will be written to stdout.
     */
    public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
    {
        @Override
        @SuppressWarnings("UseOfSystemOutOrSystemErr")
        public void process(Object t)
        {
            String s;
            if (stringFormat == null) {
                s = t.toString();
            } else {
                s = String.format(stringFormat, t);
            }
            if (!silent) {
                System.out.println(s);
            }
            if (debug) {
                logger.info(s);
            }
        }
    };
    public boolean silent = false;

    /**
     * @return the silent
     */
    public boolean isSilent()
    {
        return silent;
    }

    /**
     * @param silent the silent to set
     */
    public void setSilent(boolean silent)
    {
        this.silent = silent;
    }

    /**
     * When set to true, tuples are also logged at INFO level.
     */
    private boolean debug;
    /**
     * A formatter for {@link String#format}
     */
    private String stringFormat;

    public boolean isDebug()
    {
        return debug;
    }

    public void setDebug(boolean debug)
    {
        this.debug = debug;
    }

    public String getStringFormat()
    {
        return stringFormat;
    }

    public void setStringFormat(String stringFormat)
    {
        this.stringFormat = stringFormat;
    }
}
