package com.andyadc.pomelo.menagerie.acl;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

import java.util.List;

/**
 * @author andaicheng
 * @version 2017/7/19
 */
public class DefaultACLProvider implements ACLProvider {

    @Override
    public List<ACL> getDefaultAcl() {
        return ZooDefs.Ids.OPEN_ACL_UNSAFE;
    }

    @Override
    public List<ACL> getAclForPath(String path) {
        return ZooDefs.Ids.OPEN_ACL_UNSAFE;
    }
}
