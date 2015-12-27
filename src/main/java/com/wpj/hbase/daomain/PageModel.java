/*
 * Copyright (c) 2015 - 12 - 27  5 : 46 :56
 * @author wupeiji It will be
 * @Email wpjlovehome@gmail.com
 */

package com.wpj.hbase.daomain;

import java.util.List;
import java.util.Map;

/**
 * Name：PageModel
 * Time：2015/12/27 17:46
 * author：WPJ587
 * description：分页
 **/

public class PageModel {

        private Integer currentPage;
        private Integer pageSize;
        private Integer totalCount;
        private Integer totalPage;
        private List<Map<String, String>> resultList;

        public Integer getCurrentPage() {
            return currentPage;
        }

        public void setCurrentPage(Integer currentPage) {
            this.currentPage = currentPage;
        }

        public Integer getPageSize() {
            return pageSize;
        }

        public void setPageSize(Integer pageSize) {
            this.pageSize = pageSize;
        }

        public Integer getTotalCount() {
            return totalCount;
        }

        public void setTotalCount(Integer totalCount) {
            this.totalCount = totalCount;
        }

        public Integer getTotalPage() {
            return totalPage;
        }

        public void setTotalPage(Integer totalPage) {
            this.totalPage = totalPage;
        }

        public List<Map<String, String>> getResultList() {
            return resultList;
        }

        public void setResultList(List<Map<String, String>> resultList) {
            this.resultList = resultList;
        }

    @Override
    public String toString() {
        return "PageModel{" +
                "currentPage=" + currentPage +
                ", pageSize=" + pageSize +
                ", totalCount=" + totalCount +
                ", totalPage=" + totalPage +
                ", resultList=" + resultList +
                '}';
    }
}
