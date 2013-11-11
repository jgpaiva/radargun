package org.radargun.tpcc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.tpcc.transaction.NewOrderTransaction;
import org.radargun.tpcc.transaction.OrderStatusTransaction;
import org.radargun.tpcc.transaction.PaymentTransaction;
import org.radargun.tpcc.transaction.TpccTransaction;


/**
 * @author peluso@gsd.inesc-id.pt , peluso@dis.uniroma1.it
 * @author Pedro Ruivo
 */
public class TpccTerminal {

   private static Log log = LogFactory.getLog(TpccTerminal.class);

   public final static int NEW_ORDER = 1, PAYMENT = 2, ORDER_STATUS = 3, DELIVERY = 4, STOCK_LEVEL = 5;

   public final static String[] nameTokens = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};

   private double paymentWeight;

   private double orderStatusWeight;

   private final int indexNode;

   private int localWarehouseID;

   private final TpccTools tpccTools;

   private final int localityProbability;
   
   private final int warehouseOffset;


   public TpccTerminal(double paymentWeight, double orderStatusWeight, int indexNode, int localWarehouseID,int localityProbability, int warehouseOffset) {
      this.paymentWeight = paymentWeight;
      this.orderStatusWeight = orderStatusWeight;
      this.indexNode = indexNode;
      this.localWarehouseID = localWarehouseID;
      this.localityProbability = localityProbability;
      this.warehouseOffset = warehouseOffset;
      tpccTools = TpccTools.newInstance();
      
      log.info("created " + this.toString());
   }

   public synchronized final TpccTransaction createTransaction(int type) {
      switch (type) {
         case PAYMENT:
            return new PaymentTransaction(tpccTools, indexNode, this);
         case ORDER_STATUS:
            return new OrderStatusTransaction(tpccTools, this);
         case NEW_ORDER:
            return new NewOrderTransaction(tpccTools, this);
         case DELIVERY:
         case STOCK_LEVEL:
         default:
            return null;
      }
   }

   public synchronized final long chooseWarehouse() {
	   long myWarehouse = 0;
	   long firstWh = 1;
	   long lastWh = TpccTools.NB_WAREHOUSES;
	   if(warehouseOffset >= 0) {
		   myWarehouse = ((localWarehouseID - firstWh + warehouseOffset) % (lastWh - firstWh + 1)) + firstWh;   
	   } else {
		   long mid = firstWh + (lastWh - firstWh)/2; //1 + (40-1)/2 = 1 + 19.5 = 20   
		   if(localWarehouseID> mid) {
			   firstWh = mid+1;
			   myWarehouse = ((localWarehouseID - firstWh - warehouseOffset) % (lastWh - firstWh + 1)) + firstWh;
		   }else {
			   myWarehouse = localWarehouseID;
		   }
	   }


	   if (localityProbability < 0) {
		   return tpccTools.randomNumber(1, TpccTools.NB_WAREHOUSES);
	   } else if (tpccTools.randomNumber(0, 100) < localityProbability) {
		   return myWarehouse;
	   } else {
		   long warehouseId;
		   do {
			   warehouseId = tpccTools.randomNumber(1, TpccTools.NB_WAREHOUSES);
		   }
		   while (warehouseId == myWarehouse && TpccTools.NB_WAREHOUSES > 1);
		   return warehouseId;
	   }
   }

   public synchronized final TpccTransaction choiceTransaction(boolean isPassiveReplication, boolean isTheMaster) {
      return createTransaction(chooseTransactionType(isPassiveReplication, isTheMaster));
   }

   public synchronized final int chooseTransactionType(boolean isPassiveReplication, boolean isTheMaster) {
      double transactionType = Math.min(tpccTools.doubleRandomNumber(1, 100), 100.0);

      double realPaymentWeight = paymentWeight, realOrderStatusWeight = orderStatusWeight;

      if (isPassiveReplication) {
         if (isTheMaster) {
            realPaymentWeight = paymentWeight + (orderStatusWeight / 2);
            realOrderStatusWeight = 0;
         } else {
            realPaymentWeight = 0;
            realOrderStatusWeight = 100;
         }
      }

      if (log.isDebugEnabled()) {
         log.debug("Choose transaction " + transactionType +
                         ". Payment Weight=" + realPaymentWeight + "(" + paymentWeight + ")" +
                         ", Order Status Weight=" + realOrderStatusWeight + "(" + orderStatusWeight + ")");
      }

      if (transactionType <= realPaymentWeight) {
         return PAYMENT;
      } else if (transactionType <= realPaymentWeight + realOrderStatusWeight) {
         return ORDER_STATUS;
      } else {
         return NEW_ORDER;
      }
   }

   public synchronized void change(int localWarehouseID, double paymentWeight, double orderStatusWeight) {
      setLocalWarehouseID(localWarehouseID);
      setPercentages(paymentWeight, orderStatusWeight);
   }

   public synchronized void setPercentages(double paymentWeight, double orderStatusWeight) {
      this.paymentWeight = paymentWeight;
      this.orderStatusWeight = orderStatusWeight;
   }

   public synchronized void setLocalWarehouseID(int localWarehouseID) {
      this.localWarehouseID = localWarehouseID;
   }

   @Override
   public String toString() {
      return "TpccTerminal{" +
            "paymentWeight=" + paymentWeight +
            ", orderStatusWeight=" + orderStatusWeight +
            ", localWarehouseID=" + (localWarehouseID == -1 ? "random" : localWarehouseID) +
            ", localityProbability=" + localityProbability + 
            ", warehouseOffset=" + warehouseOffset + 
            '}';
   }
}
