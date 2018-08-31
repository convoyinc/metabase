

import java.util.*;
import java.util.concurrent.atomic.*;

public class DummySetTableLastUpdated implements ISetTableLastUpdated
{
    private Map<String, ? extends Map<String, java.util.Date>> tableLastUpdated;

    public DummySetTableLastUpdated(Map<String, ? extends Map<String, java.util.Date>> tableLastUpdated)
    {
        this.tableLastUpdated = tableLastUpdated;
    }

    @Override
    public void setTableLastUpdatedRef(AtomicReference<Map<String, ? extends Map<String, java.util.Date>>> tableLastUpdatedRef)
    {
        tableLastUpdatedRef.set(tableLastUpdated);
    }

    @Override
    public void run()
    {
    }

    @Override
    public boolean isValid()
    {
        return true;
    }
}