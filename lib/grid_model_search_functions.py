import pandas as pd
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler


def grid_cv_test_model(
    model,
    model_parameters,
    data_frame,
    features_to_use,
    feature_to_predict,
    scoring_method,
    cv_train_size,
    cv_test_size,
    lag_time,
):

    steps = [("scaler", MinMaxScaler((-1, 1))), ("model", model)]
    std_parms = {f"model__{k}": v for k, v in model_parameters.items()}

    pipeline = Pipeline(steps)

    X = data_frame[features_to_use]
    y = data_frame[feature_to_predict]

    # view_splits = list(
    #     TimeSeriesSplit(
    #         n_splits=10,
    #         max_train_size=cv_train_size,
    #         test_size=cv_test_size,
    #         gap=lag_time,
    #     ).split(X)
    # )
    # for split in view_splits:
    #     print((split[0][0], split[0][-1]), (split[1][0], split[1][-1]))

    tss_splits = TimeSeriesSplit(
        n_splits=10,
        max_train_size=cv_train_size,
        test_size=cv_test_size,
        gap=lag_time,
    ).split(X)

    grid_search_cv_model = GridSearchCV(
        pipeline,
        param_grid=std_parms,
        scoring=scoring_method,
        cv=tss_splits,
        n_jobs=-1,
    )

    grid_search_cv_model.fit(X, y)
    return grid_search_cv_model.cv_results_


def iterative_grid_cv_model_testing(
    model, model_parameters, data_settings_grid_list, features_to_use
):
    results_df = pd.DataFrame()
    for data_settings in data_settings_grid_list:
        if data_settings["cv_train_size"] < data_settings["cv_test_size"]:
            continue
        try:
            res = grid_cv_test_model(
                model,
                model_parameters=model_parameters,
                data_frame=data_settings["data_frame"][0],
                cv_train_size=data_settings["cv_train_size"],
                cv_test_size=data_settings["cv_test_size"],
                features_to_use=features_to_use,
                lag_time=data_settings["lag_time"],
                feature_to_predict=data_settings["feature_to_predict"],
                scoring_method=data_settings["scoring_method"],
            )

            res_df = pd.DataFrame(res)
            res_df["data_frame"] = data_settings["data_frame"][1]
            res_df["cv_train_size"] = data_settings["cv_train_size"]
            res_df["cv_test_size"] = data_settings["cv_test_size"]
            res_df["lag_time"] = data_settings["lag_time"]
            res_df["scoring_method"] = data_settings["scoring_method"]
            res_df["features_to_use"] = ",".join(features_to_use)

            results_df = pd.concat([results_df, res_df])
        except:
            pass
    return results_df
