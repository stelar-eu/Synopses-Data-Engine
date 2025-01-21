package infore.SDE.messages;


import java.io.Serializable;

/**
 * Just a dummy class to create a superclass to use as type instead of generic in Collectors<T>.
 * It is inherited by messages.Estimation and messages.Message .
 */
public abstract class SDEOutput {

    public abstract int getNoOfP();

}
