package io.openmessaging.demo;

/**
 * Created by mst on 2017/5/22.
 */
public class FileSlice {
    public int slice;
    public String bucket;
    public FileSlice(String bucket,int slice) {
        this.slice = slice;
        this.bucket = bucket;
    }
    @Override
    public boolean equals(Object o){
        if(this == o)
            return true;
        if(o instanceof FileSlice) {
            return ((FileSlice) o).slice == slice && ((FileSlice) o).bucket.equals(bucket);
        }
        return false;
    }
    @Override
    public int hashCode(){
        return slice*31 + bucket.hashCode();
    }
}
