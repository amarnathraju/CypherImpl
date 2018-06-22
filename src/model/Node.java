package model;

import java.util.HashSet;
import java.util.Set;

public class Node {
    private String id, name;
    private Set<String> labels = new HashSet<>();

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<String> getLabels() {
        return labels;
    }

    public void setLabels(Set<String> labels) {
        this.labels = labels;
    }

    public void addLabels(Set<String> newLabels){
        this.labels.addAll(newLabels);
    }

    public void addLabel(String newLabel){
        this.labels.add(newLabel);
    }

    @Override
    public String toString() {
        return "Node{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", labels=" + labels +
                '}';
    }
}
