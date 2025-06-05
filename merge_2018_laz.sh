#!/bin/bash

OUTDIR="/home/jehayes/helens/final_las_reproj/2018"
mkdir -p "${OUTDIR}"

pdal merge \
  /home/jehayes/helens/data/als/lpcs/WA_FEMAHQ_B2A_2018/USGS_LPC_WA_FEMAHQ_2018_D18_10TES6115.laz \
  /home/jehayes/helens/data/als/lpcs/WA_FEMAHQ_B2A_2018/USGS_LPC_WA_FEMAHQ_2018_D18_10TES6116.laz \
  /home/jehayes/helens/data/als/lpcs/WA_FEMAHQ_B2A_2018/USGS_LPC_WA_FEMAHQ_2018_D18_10TES6117.laz \
  /home/jehayes/helens/data/als/lpcs/WA_FEMAHQ_B2A_2018/USGS_LPC_WA_FEMAHQ_2018_D18_10TES6215.laz \
  /home/jehayes/helens/data/als/lpcs/WA_FEMAHQ_B2A_2018/USGS_LPC_WA_FEMAHQ_2018_D18_10TES6216.laz \
  /home/jehayes/helens/data/als/lpcs/WA_FEMAHQ_B2A_2018/USGS_LPC_WA_FEMAHQ_2018_D18_10TES6217.laz \
  /home/jehayes/helens/data/als/lpcs/WA_FEMAHQ_B2A_2018/USGS_LPC_WA_FEMAHQ_2018_D18_10TES6315.laz \
  /home/jehayes/helens/data/als/lpcs/WA_FEMAHQ_B2A_2018/USGS_LPC_WA_FEMAHQ_2018_D18_10TES6316.laz \
  /home/jehayes/helens/data/als/lpcs/WA_FEMAHQ_B2A_2018/USGS_LPC_WA_FEMAHQ_2018_D18_10TES6317.laz \
  "${OUTDIR}/2018_merged_6339.laz"
