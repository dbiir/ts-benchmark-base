package cn.edu.ruc.adapter;

import cn.edu.ruc.base.*;

import java.util.Random;

public class TestAdatper implements DBAdapter {
    private Random random=new Random();
    @Override
    public void initDataSource(TsDataSource ds, TsParamConfig tspc) {

    }

    @Override
    public Object preWrite(TsWrite tsWrite) {
        return null;
    }

    @Override
    public Status execWrite(Object write) {
        return Status.OK(random.nextInt(),random.nextInt(10000));
    }

    @Override
    public Object preQuery(TsQuery tsQuery) {
        return null;
    }

    @Override
    public Status execQuery(Object query) {
        return Status.OK(random.nextInt(),random.nextInt(10000));
    }

    @Override
    public void closeAdapter() {

    }
}
