package org.alghimo.sparkPipelines

import org.alghimo.sparkPipelines.dataManager.DataManager

/**
  * Created by alghimo on 10/30/2016.
  */
trait WithManagedData {
    def dataManager: DataManager
}
