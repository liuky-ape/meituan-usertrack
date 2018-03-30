package com.ibeifeng.senior.usertrack.dao.factory;

import com.ibeifeng.senior.usertrack.dao.ITaskDAO;
import com.ibeifeng.senior.usertrack.dao.impl.TaskDAOImpl;

/**
 * DAO工厂类
 */
public class DAOFactory {

    /**
     * 获取任务管理DAO
     *
     * @return DAO
     */
    public static ITaskDAO getTaskDAO() {
        return new TaskDAOImpl();
    }

}
