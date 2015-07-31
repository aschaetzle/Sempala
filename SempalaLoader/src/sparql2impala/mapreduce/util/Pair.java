package sparql2impala.mapreduce.util;

public class Pair<X extends Comparable<X>, Y extends Comparable<Y>> implements
		Comparable<Pair<X, Y>> {
	private X first;
	private Y last;

	public Pair(X first, Y last) {
		this.first = first;
		this.last = last;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Pair<X, Y> other = (Pair<X, Y>) obj;
		if (first == null) {
			if (other.first != null)
				return false;
		} else if (!first.equals(other.first))
			return false;
		if (last == null) {
			if (other.last != null)
				return false;
		} else if (!last.equals(other.last))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "[" + this.first + ", " + this.last + "]";
	}

	@Override
	public int compareTo(Pair<X, Y> o) {
		if (this.first.compareTo(o.first) == 0) {
			return this.last.compareTo(o.last);
		} else {
			return this.first.compareTo(o.first);
		}

	}

}
