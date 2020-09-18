import functions
import menu

var = 1;
change_cell = 0;

menu.cabecalho()

while True:

    if(var == 2): break

    elif(var == 1): str_choosen_instrument, choosen_instrument = menu.menu_1()

    while (not ((change_cell == 'Y') or (change_cell == 'y'))):
        change_cell=menu.menu_2(str_choosen_instrument)
        if not ((change_cell == 'Y') or (change_cell == 'y')) : menu.menu_change(str_choosen_instrument, change_cell)


    else :
        menu.msg_choose_raw_data_folder(str_choosen_instrument)
        folder_selected=functions.raw_data_directory()

        if(choosen_instrument == 1):
            functions.CPC(folder_selected)
            menu.msg_execution_finished()
        elif(choosen_instrument == 2):
            functions.SMPS(folder_selected)
            menu.msg_execution_finished()
        elif(choosen_instrument == 3):
            functions.Termo49i(folder_selected)
            menu.msg_execution_finished()
        else:
            functions.AE33(folder_selected)
            menu.msg_execution_finished()

    change_cell = 0;
    var=menu.menu_3()
