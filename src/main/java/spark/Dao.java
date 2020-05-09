package spark;

import java.util.List;

public interface Dao<E> {
	void insert(E e);
	
	List<E> readAll();
}
