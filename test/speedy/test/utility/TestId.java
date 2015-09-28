package speedy.test.utility;

import speedy.utility.Size;

public class TestId implements Comparable<TestId> {

    private Size size;
    private String group;

    public TestId(Size size, String group) {
        this.size = size;
        this.group = group;
    }

    public int compareTo(TestId o) {
        if (this.group.equals(o.group)) {
            return size.compareTo(o.size);
        }
        return group.compareTo(o.group);
    }

    @Override
    public String toString() {
        return size + " [" + group + ']';
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        final TestId other = (TestId) obj;
        return this.toString().equals(other.toString());
    }

}
