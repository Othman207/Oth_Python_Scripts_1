import pandas as pd
import numpy as np

# Podor
jandf = pd.read_excel('/Users/othman/Desktop/Senegal/PROJET/jan.xls',"PODOR")
febdf = pd.read_excel('/Users/othman/Desktop/Senegal/PROJET/feb.xls',"PODOR")
mardf = pd.read_excel('/Users/othman/Desktop/Senegal/PROJET/mar.xls',"PODOR")
aprdf = pd.read_excel('/Users/othman/Desktop/Senegal/PROJET/apr.xls',"PODOR")
maydf = pd.read_excel('/Users/othman/Desktop/Senegal/PROJET/may.xls',"PODOR")
jundf = pd.read_excel('/Users/othman/Desktop/Senegal/PROJET/jun.xls',"PODOR")
juldf = pd.read_excel('/Users/othman/Desktop/Senegal/PROJET/jul.xls',"PODOR")
augdf = pd.read_excel('/Users/othman/Desktop/Senegal/PROJET/aug.xls',"PODOR")
sepdf = pd.read_excel('/Users/othman/Desktop/Senegal/PROJET/sep.xls',"PODOR")
octdf = pd.read_excel('/Users/othman/Desktop/Senegal/PROJET/oct.xls',"PODOR")
novdf = pd.read_excel('/Users/othman/Desktop/Senegal/PROJET/nov.xls',"PODOR")
decdf = pd.read_excel('/Users/othman/Desktop/Senegal/PROJET/dec.xls',"PODOR")

# Extract rows with disp
jandf2=jandf.iloc[[2, 8, 15, 22, 29, 35, 42, 49, 55, 62, 68, 74, 80, 86, 92, 98, 104, 110, 116, 122, 128, 134, 140, 146, 152, 158, 164, 170, 176, 182, 188, 194, 200, 206]]
febdf2=febdf.iloc[[2, 8, 15, 22, 29, 35, 42, 49, 55, 62, 68, 74, 80, 86, 92, 98, 104, 110, 116, 122, 128, 134, 140, 146, 152, 158, 164, 170, 176, 182, 188, 194, 200, 206]]
mardf2=mardf.iloc[[2, 8, 15, 22, 29, 35, 42, 49, 55, 62, 68, 74, 80, 86, 92, 98, 104, 110, 116, 122, 128, 134, 140, 146, 152, 158, 164, 170, 176, 182, 188, 194, 200, 206]]
aprdf2=aprdf.iloc[[2, 8, 15, 22, 29, 35, 42, 49, 55, 62, 68, 74, 80, 86, 92, 98, 104, 110, 116, 122, 128, 134, 140, 146, 152, 158, 164, 170, 176, 182, 188, 194, 200, 206]]
maydf2=maydf.iloc[[2, 8, 15, 22, 29, 35, 42, 49, 55, 62, 68, 74, 80, 86, 92, 98, 104, 110, 116, 122, 128, 134, 140, 146, 152, 158, 164, 170, 176, 182, 188, 194, 200, 206]]
jundf2=jundf.iloc[[2, 8, 15, 22, 29, 35, 42, 49, 55, 62, 68, 74, 80, 86, 92, 98, 104, 110, 116, 122, 128, 134, 140, 146, 152, 158, 164, 170, 176, 182, 188, 194, 200, 206]]
juldf2=juldf.iloc[[2, 8, 15, 22, 29, 35, 42, 49, 55, 62, 68, 74, 80, 86, 92, 98, 104, 110, 116, 122, 128, 134, 140, 146, 152, 158, 164, 170, 176, 182, 188, 194, 200, 206]]
augdf2=augdf.iloc[[2, 8, 15, 22, 29, 35, 42, 49, 55, 62, 68, 74, 80, 86, 92, 98, 104, 110, 116, 122, 128, 134, 140, 146, 152, 158, 164, 170, 176, 182, 188, 194, 200, 206]]
sepdf2=sepdf.iloc[[2, 8, 15, 22, 29, 35, 42, 49, 55, 62, 68, 74, 80, 86, 92, 98, 104, 110, 116, 122, 128, 134, 140, 146, 152, 158, 164, 170, 176, 182, 188, 194, 200, 206]]
octdf2=octdf.iloc[[2, 8, 15, 22, 29, 35, 42, 49, 55, 62, 68, 74, 80, 86, 92, 98, 104, 110, 116, 122, 128, 134, 140, 146, 152, 158, 164, 170, 176, 182, 188, 194, 200, 206]]
novdf2=novdf.iloc[[2, 8, 15, 22, 29, 35, 42, 49, 55, 62, 68, 74, 80, 86, 92, 98, 104, 110, 116, 122, 128, 134, 140, 146, 152, 158, 164, 170, 176, 182, 188, 194, 200, 206]]
decdf2=decdf.iloc[[2, 8, 15, 22, 29, 35, 42, 49, 55, 62, 68, 74, 80, 86, 92, 98, 104, 110, 116, 122, 128, 134, 140, 146, 152, 158, 164, 170, 176, 182, 188, 194, 200, 206]]

# Reset index
jandf2=jandf2.reset_index(drop=True)
febdf2=febdf2.reset_index(drop=True)
mardf2=mardf2.reset_index(drop=True)
aprdf2=aprdf2.reset_index(drop=True)
maydf2=maydf2.reset_index(drop=True)
jundf2=jundf2.reset_index(drop=True)
juldf2=juldf2.reset_index(drop=True)
augdf2=augdf2.reset_index(drop=True)
sepdf2=sepdf2.reset_index(drop=True)
octdf2=octdf2.reset_index(drop=True)
novdf2=novdf2.reset_index(drop=True)
decdf2=decdf2.reset_index(drop=True)



df3 = pd.read_excel('/Users/othman/Desktop/Senegal/PROJET/jan.xls',"PODOR",usecols=['ANTIGENES'])

df3=df3.dropna().reset_index(drop=True)

df3=df3.drop(df3.index[[25,35,36,37,38]])
# df3.join(df2)
#
# frames = [df3, df2]
# result = pd.concat(frames)


# Use numpy tile to repeat rows for dframe in a new dataframe x to create an array y
x = df3
m = df2
# Concatenate arrays y and c into a dataframe df1
jandff = pd.DataFrame(np.hstack((x,jandf2)))
febdff = pd.DataFrame(np.hstack((x,febdf2)))
mardff = pd.DataFrame(np.hstack((x,mardf2)))
aprdff = pd.DataFrame(np.hstack((x,aprdf2)))
maydff = pd.DataFrame(np.hstack((x,maydf2)))
jundff = pd.DataFrame(np.hstack((x,juldf2)))
juldff = pd.DataFrame(np.hstack((x,juldf2)))
augdff = pd.DataFrame(np.hstack((x,augdf2)))
sepdff = pd.DataFrame(np.hstack((x,sepdf2)))
octdff = pd.DataFrame(np.hstack((x,octdf2)))
novdff = pd.DataFrame(np.hstack((x,novdf2)))
decdff = pd.DataFrame(np.hstack((x,decdf2)))

# Remove empty column
podorjan21 = jandff.drop(jandff.columns[[1]],axis = 1)
podorfeb21 = febdff.drop(febdff.columns[[1]],axis = 1)
podormar21 = mardff.drop(mardff.columns[[1]],axis = 1)
podorapr21 = aprdff.drop(aprdff.columns[[1]],axis = 1)
podormay21 = maydff.drop(maydff.columns[[1]],axis = 1)
podorjun21 = jundff.drop(jundff.columns[[1]],axis = 1)
podorjul21 = juldff.drop(juldff.columns[[1]],axis = 1)
podoraug21 = augdff.drop(augdff.columns[[1]],axis = 1)
podorsep21 = sepdff.drop(sepdff.columns[[1]],axis = 1)
podoroct21 = octdff.drop(octdff.columns[[1]],axis = 1)
podornov21 = novdff.drop(novdff.columns[[1]],axis = 1)
podordec21 = decdff.drop(decdff.columns[[1]],axis = 1)

# Add Monthly Report Column
podorjan21['Monthly reports'] = 'Jan-21'
podorfeb21['Monthly reports'] = 'Feb-21'
podormar21['Monthly reports'] = 'Mar-21'
podorapr21['Monthly reports'] = 'Apr-21'
podormay21['Monthly reports'] = 'May-21'
podorjun21['Monthly reports'] = 'Jun-21'
podorjul21['Monthly reports'] = 'Jul-21'
podoraug21['Monthly reports'] = 'Aug-21'
podorsep21['Monthly reports'] = 'Sep-21'
podoroct21['Monthly reports'] = 'Oct-21'
podornov21['Monthly reports'] = 'Nov-21'
podordec21['Monthly reports'] = 'Dec-21'

# Add all Podor into a single df
podor_df = pd.concat([podorjan21, podorfeb21, podormar21, podorapr21, podormay21, podorjun21, podorjul21, podoraug21, podorsep21, podoroct21, podornov21, podordec21])

#colnames = jandf2.columns.values.tolist()

e = {0:'Facility', 2:'Hep-B', 3:'BCG', 4:'OPV', 5:'IPV', 6:'PENTA', 7:'PCV', 8:'ROTA', 9:'MV', 10:'YF', 11:'VAT', 12:'HPV', 13:'0.05ml', 14:'0.5ml', 15:'2ml Syr', 16:'5ml Syr', 17:'BS'}

# d = {5:'five', 6:'six', 7:'seven', 8:'eight'}
podor_df = podor_df.rename(e, axis=1)

podor_df.to_csv("/Users/othman/Downloads/podor21.csv")