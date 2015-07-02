package acromusashi.stream;

import static org.hamcrest.CoreMatchers.is;

import org.hamcrest.Matcher;

/**
 * Class Matching Matcher in JUnit
 * 
 * @author kimura
 */
public class ClassMatcher
{
    /**
     * Constructor
     */
    private ClassMatcher()
    {}

    /**
     * Match Class type
     * 
     * @param clazz Class
     * @return result
     */
    public static Matcher<Class<?>> isSameClassAs(Class<?> clazz)
    {
        @SuppressWarnings("unchecked")
        Matcher<Class<?>> result = (Matcher<Class<?>>) (Matcher<?>) is((Object) clazz);

        return result;
    }
}
