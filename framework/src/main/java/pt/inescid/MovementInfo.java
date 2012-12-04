package pt.inescid;

import pt.inescid.utils.CounterMap;

public class MovementInfo {
	private final long startTime;
	private Long endTime = null;
	private final int startKeys;
	private Integer endKeys = null;
	private final int startSize;
	private Integer endSize = null;
	private CounterMap<Object> keysMoved;
	private Long newViewId = null;

	public MovementInfo(int startKeys, long startTime, int startSize) {
		this.startKeys = startKeys;
		this.startTime = startTime;
		this.startSize = startSize;
	}

	public void setEndKeys(int endKeys) {
		this.endKeys = endKeys;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	public void setEndSize(int endSize) {
		this.endSize = endSize;
	}

	public void setKeysMoved(CounterMap<Object> keysMoved) {
		this.keysMoved = keysMoved;
	}

	public int getStartSize() {
		return startSize;
	}

	public int getStartKeys() {
		return startKeys;
	}

	public long getStartTime() {
		return startTime;
	}

	public Long getEndTime() {
		return endTime;
	}

	public void setEndTime(Long endTime) {
		this.endTime = endTime;
	}

	public Integer getEndKeys() {
		return endKeys;
	}

	public void setEndKeys(Integer endKeys) {
		this.endKeys = endKeys;
	}

	public Integer getEndSize() {
		return endSize;
	}

	public void setEndSize(Integer endSize) {
		this.endSize = endSize;
	}

	public CounterMap<Object> getKeysMoved() {
		return keysMoved;
	}

	public String toString() {
		return " startTime: " + startTime + " startKeys:" + startKeys + " startSize:" + startSize
				+ " endTime:" + endTime + " endKeys:" + endKeys + " endSize:" + endSize + " keysMoved:"
				+ (keysMoved != null ? keysMoved.getSum() : null);
	}

	public void setEndView(Long newViewId) {
		this.newViewId = newViewId;
	}

	public Long getNewViewId() {
		return newViewId;
	}

	public void setNewViewId(Long newViewId) {
		this.newViewId = newViewId;
	}
}
