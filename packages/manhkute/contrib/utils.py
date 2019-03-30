def check_time(on_time, off_time, current_time):
    if on_time < off_time:
        if current_time >= on_time and current_time <= off_time:
            return True
        else:
            return False
    elif on_time > off_time:
        if current_time >= on_time or current_time <= off_time:
            return True
        else:
            return False
    else:
        if current_time == on_time:
            return True
        else:
            return False

