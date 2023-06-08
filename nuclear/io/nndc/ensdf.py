class ENSDFReader:

    def __init__(self, fname):

        #### Still seemingly missing several bits of data from NNDC ###
        raise NotImplementedError('Not quite working yet')

    @staticmethod
    def _gamma_to_dataframe(gamma_list):
        columns = ['level_1_id', 'level_2_id', 'parent_id', 'daughter_id',
                   'energy', 'energy_uncert', 'intensity', 'intensity_uncert',
                   'electron_conversion_intensity', 'electron_conversion_intensity_uncert',
                   'total_transition_intensity', 'total_transition_intensity_uncert']
        columns += ['{0}_electron_conversion_intensity'.format(item) for item in 'klm']

        return pd.DataFrame(data=gamma_list, columns=columns)

    @staticmethod
    def _alpha_to_dataframe(alpha_list):
        if alpha_list == []:
            return None
        else:
            raise NotImplementedError('Not implemented yet')

        # parent nuclide id in state_id form
        # child nuclide id in state_id form
        # alpha energy
        # alpha intensity in percent of total alphas

    @staticmethod
    def _beta_minus_to_dataframe(beta_minus_list):
        if beta_minus_list == []:
            return None
        columns = ['parent_id', 'child_id', 'endpoint_energy', 'average_energy', 'intensity']
        return pd.DataFrame(beta_minus_list, columns=columns)

    @staticmethod
    def _beta_plus_to_dataframe(beta_plus_list):
        if beta_plus_list == []:
            return None
        columns = ['parent_id', 'child_id', 'endpoint_energy', 'average_energy', 'intensity',
                   'electron_capture_intensity']
        columns += ['{0}_electron_conversion_intensity'.format(item) for item in 'klm']
        return pd.DataFrame(beta_plus_list, columns=columns)
