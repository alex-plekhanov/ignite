package org.apache.ignite.springdata.misc;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

public class PersonTxImpl implements PersonTx {
    @Autowired
    private PersonRepository personRepository;

    @Transactional
    @Override public void saveInTxAndWaitLatch(Integer key, Person entity, CountDownLatch latch) {
        personRepository.save(key, entity);

        try {
            latch.await();
        }
        catch (InterruptedException e) {
            throw new IgniteException(e);
        }
    }
}
