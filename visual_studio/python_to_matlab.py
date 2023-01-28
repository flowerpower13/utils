
#documentation
#https://github.com/mathworks/matlab-engine-for-python/


#import
import matlab.engine

#start
eng = matlab.engine.start_matlab()

#run script
eng.moms_and_cluster_cov(nargout=0)

#quit
eng.quit()

print("done")