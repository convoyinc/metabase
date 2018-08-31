
import java.util.*;
import java.util.concurrent.atomic.*;


public interface ISetTableLastUpdated extends Runnable
{
    void setTableLastUpdatedRef(AtomicReference<Map<String, ? extends Map<String, java.util.Date>>> tableLastUpdatedRef);

    boolean isValid();
}