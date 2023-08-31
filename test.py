import numpy as np
import pandas as pd

pd.set_option('display.max_rows', 6)

from differences import simulate_data, ATTgt
panel_data = simulate_data()  # generate data

panel_data
att_gt = ATTgt(data=panel_data, cohort_name='cohort')
att_gt.fit(
    formula='y ~ x0',
)
x=att_gt.plot(
    configure_axisX={'format': 'c'},
    width=800,
    height=700,
    )


width=800,
height=700

filepath="zhao/article/figures/graph0.png"

#plot
x=att_gt.plot(
    configure_axisX={'format': 'c'},
    width=width,
    height=height,
    save_fname=filepath
    )
x.show()

x.show()