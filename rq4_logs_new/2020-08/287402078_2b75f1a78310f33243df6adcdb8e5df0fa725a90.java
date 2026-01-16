package com.minesweeper.api.domain;

import java.io.Serializable;
import java.util.List;

import com.minesweeper.api.domain.enume.GameStatus;

/**
 * A Game Request
 */
public class GameRequest implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private String id;
	
    private String name;
    
    private Integer x;

    private Integer y;
    
    private List<List<Locker>> lockers;
    
    private Integer minesCount;

    private GameStatus status;
    

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public GameRequest name(String name) {
        this.name = name;
        return this;
    }

    public void setName(String name) {
        this.name = name;
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "Game{" +
            ", name='" + getName() + "'" +
            "}";
    }

	public List<List<Locker>> getLockers() {
		return lockers;
	}

	public void setLockers(List<List<Locker>> lockers) {
		this.lockers = lockers;
	}

	public Integer getMinesCount() {
		return minesCount;
	}

	public void setMinesCount(Integer minesCount) {
		this.minesCount = minesCount;
	}

	public Integer getX() {
		return x;
	}

	public void setX(Integer x) {
		this.x = x;
	}

	public Integer getY() {
		return y;
	}

	public void setY(Integer y) {
		this.y = y;
	}

	public GameStatus getStatus() {
		return status;
	}

	public void setStatus(GameStatus gameStatus) {
		this.status = gameStatus;
	}
	
}