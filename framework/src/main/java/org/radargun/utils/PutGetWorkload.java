package org.radargun.utils;

import org.radargun.CacheWrapper;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Keeps information about the Put/Get workload, i.e, the percentage of
 * write transactions
 * 
 * @author João Paiva
 */
public class PutGetWorkload {

   private int writeTxPercentage;

   public final OperationIterator chooseTransaction(CacheWrapper cacheWrapper,
         Random random) {
      boolean readOnly = cacheWrapper.canExecuteReadOnlyTransactions()
            && (!cacheWrapper.canExecuteWriteTransactions() || random
                  .nextInt(100) >= writeTxPercentage);
      if (readOnly) {
         return new OperationIterator(true);
      } else {
         return new OperationIterator(false);
      }
   }

   public final void setWriteTxPercentage(int value) {
      this.writeTxPercentage = value;
   }

   public int getWriteTxPercentage() {
      return this.writeTxPercentage;
   }

   @Override
   public String toString() {
      return "TransactionWorkload{" + "writeTxPercentage=" + writeTxPercentage
            + '}';
   }

   public static class OperationIterator {
      private final boolean isReadOnly;

      private OperationIterator(boolean readOnly) {
         isReadOnly = readOnly;
      }

      public final boolean isReadOnly() {
         return isReadOnly;
      }
   }
}
