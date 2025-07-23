import traceback


class ExpressExceptionUtils:
    @staticmethod
    def get_stack_trace(e, print_row=0) -> str:
        tb_lines = traceback.format_exception(type(e), e, e.__traceback__)
        if print_row == 0 or print_row > len(tb_lines):
            print_row = len(tb_lines)

        # Create a string builder (list) to accumulate the stack trace string
        str_builder = []

        # First line should contain the exception details
        first_line = tb_lines[0]
        str_builder.append(first_line)

        for i in range(1, print_row):
            str_builder.append(tb_lines[i])

        return ''.join(str_builder)
