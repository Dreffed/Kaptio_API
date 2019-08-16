from sys import version_info
if version_info.major == 2:
    import Tkinter as tk
elif version_info.major == 3:
    import tkinter as tk
import json

master = tk.Tk()

listbox = tk.Listbox(master)
listbox.pack()

listbox2 = tk.Listbox(master)

def moveDown(source, dest, remove=True):
    move_text = source.selection_get()
    print(move_text)
    if move_text:
        curindex = int(source.curselection()[0])
        if remove:
            source.delete(curindex)
        dest.insert(tk.END, move_text)

moveBtn = tk.Button(master, text="Move Down", command=lambda: moveDown(listbox, listbox2, False))
moveBtn.pack()

moveBack = tk.Button(master, text="Move Back", command=lambda: moveDown(listbox2, listbox, True))
moveBack.pack()

listbox2.pack()

for item in ["one", "two", "three", "four"]:
    listbox.insert(tk.END, item)

tk.mainloop()